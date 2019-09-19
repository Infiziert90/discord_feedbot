#!/usr/bin/env python3
# Copyright (c) 2016-2017 Eric Eisenhart
# This software is released under an MIT-style license.
# See LICENSE.md for full details.

import asyncio
import logging
import os
import random
import re
import sqlite3
import sys
import time
import warnings
import aiohttp
import discord
import feedparser
import logging.handlers
from configparser import ConfigParser
from datetime import datetime
from importlib import reload
from urllib.parse import urljoin
from aiohttp.web_exceptions import HTTPForbidden, HTTPNotModified
from dateutil.parser import parse as parse_datetime
from html2text import HTML2Text

__version__ = "3.2.0r"

PROG_NAME = "feedbot"
USER_AGENT = f"{PROG_NAME}{__version__}"

SQL_CREATE_FEED_INFO_TBL = """
CREATE TABLE IF NOT EXISTS feed_info (
    feed text PRIMARY KEY,
    url text UNIQUE,
    lastmodified text,
    etag text
)
"""

SQL_CREATE_FEED_ITEMS_TBL = """
CREATE TABLE IF NOT EXISTS feed_items (
    id text PRIMARY KEY,
    published text,
    title text,
    url text,
    reposted text
)
"""

SQL_CLEAN_OLD_ITEMS = """
DELETE FROM feed_items WHERE (julianday() - julianday(published)) > 365
"""

if not sys.version_info[:2] >= (3, 6):
    print("Error: requires python 3.6 or newer")
    exit(1)


def get_config():
    ini_config = ConfigParser()
    ini_config.read(["feed2discord.ini"])

    debug = ini_config["MAIN"].getint("debug", 0)
    if debug:
        os.environ["PYTHONASYNCIODEBUG"] = "1"
        # The AIO modules need to be reloaded because of the new env var
        reload(asyncio)
        reload(aiohttp)
        reload(discord)

    if debug >= 3:
        log_level = logging.DEBUG
    elif debug >= 2:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING

    logging.basicConfig(level=log_level)
    log = logging.getLogger(__name__)
    # suppress poll infos from asyncio
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    log.addHandler(logging.handlers.TimedRotatingFileHandler("output.log", when='W0', backupCount=3))
    log.setLevel(log_level)
    warnings.resetwarnings()

    return ini_config, log


def get_timezone(ini_config):
    import pytz

    tzstr = ini_config["MAIN"].get("timezone", "utc")
    # This has to work on both windows and unix
    try:
        timezone = pytz.timezone(tzstr)
    except pytz.UnknownTimeZoneError:
        timezone = pytz.utc

    return timezone


def get_feeds_config(ini_config):
    feeds = list(ini_config.sections())

    # remove non-feed sections
    feeds.remove("MAIN")
    feeds.remove("CHANNELS")

    return feeds


def get_sqlite_connection(ini_config):
    db_path = ini_config["MAIN"].get("db_path", "feed2discord.db")
    conn = sqlite3.connect(db_path)

    # If our two tables don't exist, create them.
    conn.execute(SQL_CREATE_FEED_INFO_TBL)
    conn.execute(SQL_CREATE_FEED_ITEMS_TBL)

    # Clean out *some* entries that are over 1 year old...
    # Doing this cleanup at start time because some feeds
    # do contain very old items and we don't want to keep
    # re-evaluating them.
    conn.execute(SQL_CLEAN_OLD_ITEMS)

    return conn


# Make main, timezone, logger, config global, since used everywhere/anywhere
config, logger = get_config()
MAIN = config['MAIN']
TIMEZONE = get_timezone(config)

# Crazy workaround for a bug with parsing that doesn't apply on all
# pythons:
feedparser.PREFERRED_XML_PARSERS.remove('drv_libxml2')

# global discord client object
client = discord.Client()


def extract_best_item_date(item, tzinfo):
    """
    This function loops through all the common date fields for an item in a feed,
    and extracts the "best" one. Falls back to "now" if nothing is found.
    """
    fields = ("published", "pubDate", "date", "created", "updated")
    for date_field in fields:
        if date_field in item and len(item[date_field]) > 0:
            try:
                date_obj = parse_datetime(item[date_field])

                if date_obj.tzinfo is None:
                    tzinfo.localize(date_obj)

                return date_obj
            except Exception:
                pass

    # No potentials found, default to current timezone's "now"
    return tzinfo.localize(datetime.now())


def process_field(field, item, feed, channel):
    """
    This looks at the field from the config, and returns the processed string
    naked item in fields: return that field from the feed item
    *, **, _, ~, `, ```: markup the field and return it from the feed item
    " around the field: string literal
    Added new @, turns each comma separated tag into a group mention
    """
    logger.debug(f"{feed}:process_field:{field}: started")

    item_url_base = feed.get('item_url_base', None)
    if field == 'guid' and item_url_base is not None:
        if 'guid' in item:
            return item_url_base + item['guid']
        else:
            logger.error('process_field:guid:no such field; try show_sample_entry.py on feed')
            return ""

    logger.debug(f"{feed}:process_field:{field}: checking regexes")
    stringmatch = re.match('^"(.+?)"$', field)
    highlightmatch = re.match('^([*_~<]+)(.+?)([*_~>]+)$', field)
    bigcodematch = re.match('^```(.+)```$', field)
    codematch = re.match('^`(.+)`$', field)

    tagmatch = re.match('^@(.+)$', field)  # new tag regex

    if stringmatch is not None:
        # Return an actual string literal from config:
        logger.debug(f"{feed}:process_field:{field}:isString")
        return stringmatch.group(1)  # string from config
    elif highlightmatch is not None:
        logger.debug(f"{feed}:process_field:{field}:isHighlight")

        # If there's any markdown on the field, return field with that
        # markup on it:
        begin, field, end = highlightmatch.groups()
        if field in item:
            if field == "link":
                url = urljoin(feed.get("feed-url"), item[field])
                return begin + url + end
            else:
                return begin + item[field] + end
        else:
            logger.error(f"process_field:{field}:no such field")
            return ""

    elif bigcodematch is not None:
        logger.debug(f"{feed}:process_field:{field}:isCodeBlock")

        # Code blocks are a bit different, with a newline and stuff:
        field = bigcodematch.group(1)
        if field in item:
            return "```\n{item[field]}\n```"
        else:
            logger.error(f"process_field:{field}:no such field")
            return ""

    elif codematch is not None:
        logger.debug(f"{feed}:process_field:{field}:isCode")

        # Since code chunk can't have other highlights, also do them
        # separately:
        field = codematch.group(1)
        if field in item:
            return f"`{item[field]}`"
        else:
            logger.error(f"process_field:{field}:no such field")
            return ""

    elif tagmatch is not None:
        logger.debug(f"{feed}:process_field:{field}:isTag")
        field = tagmatch.group(1)
        if field in item:
            # Assuming tags are ', ' separated
            taglist = item[field].split(', ')
            # Iterate through channel roles, see if a role is mentionable and
            # then substitute the role for its id
            for role in client.get_channel(channel['id']).server.roles:
                rn = str(role.name)
                taglist = [
                    f"<@&{role.id}>" if rn == str(i) else i for i in taglist
                ]
                return ", ".join(taglist)
        else:
            logger.error(f"process_field:{field}:no such field")
            return ""

    else:
        logger.debug(f"{feed}:process_field:{field}:isPlain")
        # Just asking for plain field:
        if field in item:
            # If field is special field "link",
            # then use urljoin to turn relative URLs into absolute URLs
            if field == 'link':
                return urljoin(feed.get('feed_url'), item[field])
            # Else assume it's a "summary" or "content" or whatever field
            # and turn HTML into markdown and don't add any markup:
            else:
                htmlfixer = HTML2Text()
                logger.debug(htmlfixer)
                htmlfixer.ignore_links = True
                htmlfixer.ignore_images = True
                htmlfixer.ignore_emphasis = False
                htmlfixer.body_width = 1000
                htmlfixer.unicode_snob = True
                htmlfixer.ul_item_mark = '-'  # Default of "*" likely
                # to bold things, etc...
                markdownfield = htmlfixer.handle(item[field])

                # Try to strip any remaining HTML out.  Not "safe", but
                # simple and should catch most stuff:
                markdownfield = re.sub('<[^<]+?>', '', markdownfield)
                return markdownfield
        else:
            logger.error(f"process_field:{field}:no such field")
            return ""


def build_message(feed, item, channel):
    """
    This builds a message.

    Pulls the fields (trying for channel_name.fields in FEED, then fields in
    FEED, then fields in DEFAULT, then "id,description".
    fields in config is comma separate string, so pull into array.
    then just adds things, separated by newlines.
    truncates if too long.
    """
    message = ''
    fieldlist = feed.get(channel['name'] + '.fields', feed.get('fields', 'id,description')).split(',')
    # Extract fields in order
    for field in fieldlist:
        logger.debug(f"feed:item:build_message:{field}:added to message")
        message += process_field(field, item, feed, channel) + "\n"

    # Naked spaces are terrible:
    message = re.sub(' +\n', '\n', message)
    message = re.sub('\n +', '\n', message)

    # squash newlines down to single ones, and do that last...
    message = re.sub("(\n)+", "\n", message)

    if len(message) > 1800:
        message = message[:1800] + "\n... post truncated ..."
    return message


async def send_message_wrapper(async_loop, feed, channel, message):
    """ This schedules an 'actually_send_message' coroutine to run """
    async_loop.create_task(actually_send_message(channel, message, feed))
    logger.debug(f"{feed}:{channel['name']}:message scheduled")


async def actually_send_message(channel, message, feed):
    logger.debug(f"{feed}:{channel['name']}:actually sending message")
    await channel["channel_obj"].send(message)
    logger.debug(f"{feed}:{channel['name']}:message sent: {message!r}")


async def background_check_feed(conn, feed, async_loop):
    """
    The main work loop
    One of these is run for each feed.
    It's an asyncio thing. "await" (sleep or I/O) returns to main loop
    and gives other feeds a chance to run.
    """
    logger.info(f'{feed}: Starting up background_check_feed')

    # Try to wait until Discord client has connected, etc:
    await client.wait_until_ready()
    # make sure debug output has this check run in the right order...
    await asyncio.sleep(1)

    user_agent = config["MAIN"].get("user_agent", USER_AGENT)

    # just a bit easier to use...
    _feed = config[feed]

    # pull config for this feed out:
    feed_url = _feed.get('feed_url')
    rss_refresh_time = _feed.getint('rss_refresh_time', 3600)
    start_skew = _feed.getint('start_skew', rss_refresh_time)
    start_skew_min = _feed.getint('start_skew_min', 1)
    max_age = _feed.getint('max_age', 86400)

    # loop through all the channels this feed is configured to send to
    channels = []
    for key in _feed.get('channels').split(','):
        logger.debug(feed + ': adding channel ' + key)
        # stick a dict in the channels array so we have more to work with
        channels.append(
            {
                'channel_obj': client.get_channel(int(config['CHANNELS'][key])),
                'name': key,
                'id': int(config['CHANNELS'][key]),
            }
        )

    if start_skew > 0:
        sleep_time = random.uniform(start_skew_min, start_skew)
        logger.info(f'{feed}:start_skew:sleeping for {str(sleep_time)}')
        await asyncio.sleep(sleep_time)

    # Basically run forever
    while not client.is_closed():
        # And tries to catch all the exceptions and just keep going
        # (but see list of except/finally stuff below)
        try:
            logger.info(f'{feed}: processing feed')
            http_headers = {"User-Agent": user_agent}
            # Download the actual feed, if changed since last fetch

            # pull data about history of this *feed* from DB:
            cursor = conn.cursor()
            cursor.execute("select lastmodified,etag from feed_info where feed=? OR url=?", [feed, feed_url])
            data = cursor.fetchone()

            # If we've handled this feed before,
            # and we have etag from last run, add etag to headers.
            # and if we have a last modified time from last run,
            # add "If-Modified-Since" to headers.
            if data is None:  # never handled this feed before...
                logger.info(f"{feed}:looks like updated version. saving info")
                cursor.execute("REPLACE INTO feed_info (feed,url) VALUES (?,?)", [feed, feed_url])
                conn.commit()
                logger.debug(f"{feed}:feed info saved")
            else:
                logger.debug(f"{feed}:setting up extra headers for HTTP request.")
                logger.debug(data)
                lastmodified = data[0]
                etag = data[1]
                if lastmodified is not None and len(lastmodified):
                    logger.debug(f"{feed}:adding header If-Modified-Since: {lastmodified}")
                    http_headers['If-Modified-Since'] = lastmodified
                else:
                    logger.debug(f"{feed}:no stored lastmodified")
                if etag is not None and len(etag):
                    logger.debug(f"{feed}:adding header ETag: {etag}")
                    http_headers['ETag'] = etag
                else:
                    logger.debug(f"{feed}:no stored ETag")

            logger.debug(f"{feed}:sending http request for {feed_url}")
            feed_data = None
            # Send actual request.
            async with aiohttp.ClientSession() as sess:
                async with sess.get(feed_url, headers=http_headers) as http_response:
                    logger.debug(http_response)
                    # First check that we didn't get a "None" response, since that's
                    # some sort of internal error thing:
                    if http_response.status is None:
                        logger.error(f"{feed}:HTTP response code is NONE")
                        http_response.close()
                        # raise not HTTPError because this is giving me NoneType errors
                        raise HTTPForbidden()
                    # Some feeds are smart enough to use that if-modified-since or
                    # etag info, which gives us a 304 status.  If that happens,
                    # assume no new items, fall through rest of this and try again
                    # later.
                    elif http_response.status == 304:
                        logger.debug(f"{feed}:data is old; moving on")
                        http_response.close()
                        raise HTTPNotModified()
                    # If we get anything but a 200, that's a problem and we don't
                    # have good data, so give up and try later.
                    # Mostly handled different than 304/not-modified to make logging
                    # clearer.
                    elif http_response.status != 200:
                        logger.debug(f"{feed}:HTTP error not 200")
                        http_response.close()
                        # raise not HTTPError because this is giving me NoneType errors
                        raise HTTPForbidden()
                    else:
                        logger.debug(f"{feed}:HTTP success")

                    # pull data out of the http response
                    logger.debug(f"{feed}:reading http response")
                    http_data = await http_response.read()

                    # parse the data from the http response with feedparser
                    logger.debug(f"{feed}:parsing http data")
                    feed_data = feedparser.parse(http_data)
                    logger.debug(f"{feed}:done fetching")

                    # If we got an ETAG back in headers, store that, so we can
                    # include on next fetch
                    if 'ETAG' in http_response.headers:
                        etag = http_response.headers['ETAG']
                        logger.debug(f"{feed}:saving etag: {etag}")
                        cursor.execute("UPDATE feed_info SET etag=? where feed=? or url=?", [etag, feed, feed_url])
                        conn.commit()
                        logger.debug(f"{feed}:etag saved")
                    else:
                        logger.debug(f"{feed}:no etag")

                    # If we got a Last-Modified header back, store that, so we can
                    # include on next fetch
                    if 'LAST-MODIFIED' in http_response.headers:
                        modified = http_response.headers['LAST-MODIFIED']
                        logger.debug(f"{feed}:saving lastmodified: {modified}")
                        cursor.execute("UPDATE feed_info SET lastmodified=? where feed=? or url=?",
                                       [modified, feed, feed_url])
                        conn.commit()
                        logger.debug(f"{feed}:saved lastmodified")
                    else:
                        logger.debug(f"{feed}:no last modified date")

            # Process all of the entries in the feed
            # Use reversed to start with end, which is usually oldest
            logger.debug(f"{feed}:processing entries")
            if feed_data is None:
                logger.error(f"{feed}:no data in feed_data")
                # raise not HTTPError because this is giving me NoneType errors
                raise HTTPForbidden()
            for item in reversed(feed_data.entries):
                logger.debug(f"{feed}:item:processing this entry:{item}")

                # Pull out the unique id, or just give up on this item.
                if 'id' in item:
                    uid = item.id
                elif 'guid' in item:
                    uid = item.guid
                elif 'link' in item:
                    uid = item.link
                else:
                    logger.error(f"{feed}:item:no id, skipping")
                    continue

                # Get our best date out, in both raw and parsed form
                pubdate = extract_best_item_date(item, TIMEZONE)
                pubdate_fmt = pubdate.strftime("%a %b %d %H:%M:%S %Z %Y")

                logger.debug(f"{feed}:item:id:{uid}")
                logger.debug(f"{feed}:item:checking database history for this item")
                # Check DB for this item
                cursor.execute("SELECT published,title,url,reposted FROM feed_items WHERE id=?", [uid])
                data = cursor.fetchone()

                # If we've never seen it before, then actually processing
                # this:
                if data is None:
                    logger.info(f"{feed}:item {uid} unseen, processing:")

                    # Store info about this item, so next time we skip it:
                    cursor.execute("INSERT INTO feed_items (id,published) VALUES (?,?)", [uid, pubdate_fmt])
                    conn.commit()

                    # Doing some crazy date math stuff...
                    # max_age is mostly so that first run doesn't spew too
                    # much stuff into a room, but is also a useful safety
                    # measure in case a feed suddenly reverts to something
                    # ancient or other weird problems...
                    time_since_published = TIMEZONE.localize(datetime.now()) - pubdate.astimezone(TIMEZONE)

                    if time_since_published.total_seconds() < max_age:
                        logger.info(f"{feed}:item:fresh and ready for parsing")

                        # Loop over all channels for this particular feed
                        # and process appropriately:
                        for channel in channels:
                            # just a bit easier to use...
                            _name = channel['name']

                            include = True
                            filter_field = _feed.get(f"{_name}.filter", _feed.get('filter_field', 'title'))
                            # Regex if channel exists
                            if f"{_name}.filter" in _feed or 'filter' in _feed:
                                logger.debug(f"{feed}:item:running filter for {_name}")
                                re_pat = _feed.get(f"{_name}.filter", _feed.get('filter', '^.*$'))
                                logger.debug(f"{feed}:item:using filter: {re_pat} on "
                                             f"{item['title']} field {filter_field}")
                                re_match = re.search(re_pat, item[filter_field])
                                if re_match is None:
                                    include = False
                                    logger.info(f"{feed}:item:failed filter for {_name}")
                            elif f"{_name}.filter_exclude" in _feed or 'filter_exclude' in _feed:
                                logger.debug(f"{feed}:item:running exclude filter for{_name}")
                                re_pat = _feed.get(f"{_name}.filter_exclude", _feed.get('filter_exclude', '^.*$'))
                                logger.debug(f"{feed}:item:using filter_exclude: {re_pat} on "
                                             f"{item['title']} field {filter_field}")
                                re_match = re.search(re_pat, item[filter_field])
                                if re_match is None:
                                    include = True
                                    logger.info(f"{feed}:item:passed exclude filter for {_name}")
                                else:
                                    include = False
                                    logger.info(f"{feed}:item:failed exclude filter for {_name}")
                            else:
                                include = True  # redundant safety net
                                logger.debug(f"{feed}:item:no filter configured for {_name}")

                            if include is True:
                                logger.debug(f"{feed}:item:building message for {_name}")
                                message = build_message(_feed, item, channel)
                                logger.debug(f"{feed}:item:sending message (eventually) to {_name}")
                                await send_message_wrapper(async_loop, feed, channel, message)
                            else:
                                logger.info(f"{feed}:item:skipping item due to not passing filter for {_name}")

                    else:
                        # Logs of debugging info for date handling stuff...
                        logger.info(f"{feed}:too old, skipping")
                        logger.debug(f"{feed}:now:now:{time.time()}")
                        logger.debug(f"{feed}:now:gmtime:{time.gmtime()}")
                        logger.debug(f"{feed}:now:localtime:{time.localtime()}")
                        logger.debug(f"{feed}:pubDate:{pubdate}")
                        logger.debug(item)
                # seen before, move on:
                else:
                    logger.debug(f"{feed}:item: {uid} seen before, skipping")

        # This is completely expected behavior for a well-behaved feed:
        except HTTPNotModified:
            logger.debug(f"{datetime.today()}:{feed}: Headers indicate feed unchanged since last time fetched:")
            logger.debug(sys.exc_info())
        # Many feeds have random periodic problems that shouldn't cause
        # permanent death:
        except HTTPForbidden:
            logger.warning(f"{datetime.today()}:{feed}: Unexpected HTTPError:")
            logger.warning(sys.exc_info())
            logger.warning(f"{datetime.today()}:{feed}: Assuming error is transient and trying again later")
        # sqlite3 errors are probably really bad and we should just totally
        # give up on life
        except sqlite3.Error as sqlerr:
            logger.error(f"{datetime.today()}:{feed}: sqlite3 error: ")
            logger.error(sys.exc_info())
            logger.error(sqlerr)
            raise
        # Ideally we'd remove the specific channel or something...
        # But I guess just throw an error into the log and try again later...
        except discord.errors.Forbidden:
            logger.error(f"{datetime.today()}:{feed}: discord.errors.Forbidden")
            logger.error(sys.exc_info())
            logger.error(f"{datetime.today()}:{feed}: Perhaps bot isn't allowed in one of the channels for this feed?")
            # raise  # or not? hmm...
        except asyncio.TimeoutError:
            logger.error(f"{datetime.today()}:{feed}: Timeout error")
        except aiohttp.ClientConnectorError:
            logger.error(f"{datetime.today()}:{feed}: Connection failed!")
        except aiohttp.ClientOSError:
            logger.error(f"{datetime.today()}:{feed}: Connection not responding!")
        except aiohttp.ServerDisconnectedError:
            logger.error(f"{datetime.today()}:{feed}: Socket closed by peer")
        # unknown error: definitely give up and die and move on
        except BaseException:
            logger.exception(f"{datetime.today()}:{feed}: Unexpected error - giving up")
            # raise  # or not? hmm...
        # No matter what goes wrong, wait same time and try again
        finally:
            logger.debug(f"{feed}:sleeping for {str(rss_refresh_time)} seconds")
            await asyncio.sleep(rss_refresh_time)


@client.event
async def on_ready():
    logger.info(f"Logged in as {client.user.name} ({client.user.id})")


def main():
    """
    Set up the tasks for each feed and start the main event loop thing.
    In this __main__ thing so can be used as library.
    """
    loop = asyncio.get_event_loop()

    feeds = get_feeds_config(config)
    conn = get_sqlite_connection(config)

    try:
        for feed in feeds:
            loop.create_task(background_check_feed(conn, feed, loop))
        if "login_token" in MAIN:
            loop.run_until_complete(client.login(MAIN.get("login_token")))
        loop.run_until_complete(client.connect())
    except BaseException:
        loop.run_until_complete(client.close())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
