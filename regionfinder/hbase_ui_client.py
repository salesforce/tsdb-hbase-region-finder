'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

from bs4 import BeautifulSoup
import requests
import os.path
import csv
import time
from threading import Timer
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse
from regionfinder import error
from regionfinder.util import logger, open_csv_r, open_csv_w

class HBaseUIClient:
    CACHE_FILENAME = 'regionfinder_ranges.cache.csv'
    EXPIRY_SECONDS = 60*60*12

    def __init__(self, instance_url, table_name, cache_dir='.', autorefresh=True):
        self.master_url = instance_url
        self.table_name = table_name
        self.cache_file = cache_dir + '/' + self.CACHE_FILENAME
        self.rs_ranges = []
        # Load region servers from file if files exist
        if self._load_ranges_from_file() is None:
            logger.info('Previous region start/stop key cache file not found, or has expired')
            logger.info('Populating region server info now...')
            self._create_rs_range_list()
        else:
            logger.info('Previous region start/stop key cache file found.')
        # Start recurring cache-refresh / file-flush task for long-running processes
        if autorefresh:
            self.active_timer = Timer(self.EXPIRY_SECONDS, self._recurring_flush)
            self.active_timer.start()

    def get_rs_of_rowkey(self, rowkey):
        for server, region, start, stop in self.rs_ranges:
            if rowkey < stop and start < rowkey:
                return (server, region)
        raise error.RegionFinderError('Could not find a region whose start/end key range contains the rowkey')

    def _load_ranges_from_file(self):
        if not os.path.isfile(self.cache_file):
            return None

        with open_csv_r(self.cache_file) as csvfile:
            last_updated = int(csvfile.readline())
            if int(time.time()) - last_updated > self.EXPIRY_SECONDS:
                return None
            reader = csv.reader(csvfile)
            self.rs_ranges = []
            for row in reader:
                self.rs_ranges.append([row[0], row[1], row[2], row[3]])
            # Handle empty cache file
            if len(self.rs_ranges) == 0:
                return None
        return self.rs_ranges

    def _recurring_flush(self):
        self._create_rs_range_list()
        self.active_timer = Timer(self.EXPIRY_SECONDS, self._recurring_flush)
        self.active_timer.start()

    def _flush_ranges_to_file(self):
        with open_csv_w(self.cache_file) as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([int(time.time())])
            writer.writerows(self.rs_ranges)
            logger.info('Finished flush to {}'.format(self.cache_file))

    def _get_region_servers(self):
        resp = requests.get(self.master_url + '/master-status')
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.select('div#tab_baseStats table tr')
        # If no rows are found, it is likely that this is a backup master
        if len(rows) == 0:
            page_h1s = soup.select('div.page-header h1')
            if len(page_h1s) == 0:
                raise error.RegionFinderError('No region servers on /master-status of {}. Unable to parse current active master.'.format(self.master_url))

            if 'backup' in page_h1s[0].text.lower():
                logger.warning('The HBase endpoint {} is not the current active master'.format(self.master_url))

                page_h4s = soup.select('section h4')
                for h4 in page_h4s:
                    if 'current active master' in h4.text.lower() and h4.find('a') is not None:
                        active_master_url = urlparse(h4.find('a').get('href'))
                        self.master_url = 'http://{}:{}'.format(active_master_url.hostname, active_master_url.port)
                        logger.warning('Parsed current active master to be {}. Retrying with this endpoint'.format(self.master_url))
                        logger.warning('It is recommended that you update your config.yaml')
                        return self._get_region_servers()

            raise error.RegionFinderError('No region servers on /master-status of {}. Unable to parse current active master.'.format(self.master_url))

        region_server_hrefs = []
        # Exclude the header and footer rows
        for row in rows[1:-1]:
            if row.find('a') is not None:
                region_server_hrefs.append('http:' + row.find('a').get('href'))
        return region_server_hrefs

    def _get_region_ranges(self, url):
        urlObject = urlparse(url)
        # newer version of the UI has the path in the href while the older one didn't
        if urlObject.path != '/rs-status':
            urlObject.path = '/rs-status';
        logger.info('Retrieving info for {}'.format(urlObject.geturl()))
        resp = requests.get(urlObject.geturl())
        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.select('div#tab_regionBaseInfo table tr')
        region_names = []
        for row in rows[1:]:
            tds = row.find_all('td')
            # 3 <td> per row indicates an older version of the UI where the "Region Name" format is
            #   tableName,timestamp,region
            if len(tds) == 3:
                if tds[0].string.strip().startswith(self.table_name + ','):
                    region_names.append([tds[0].string, tds[1].string, tds[2].string])
                else:
                    continue
            # 4 <td> per row indicates a newer version of the UI where the "Region Name" format is
            #   tableName,startKey,timestamp,region
            elif len(tds) == 4:
                full_name = tds[0].find('a').string.strip()
                name_tokens = full_name.split(',')
                if full_name.startswith(self.table_name + ','):
                    name = name_tokens[0] + ',' + name_tokens[2]
                    region_names.append([name, tds[1].string, tds[2].string])
                else:
                    continue
            else:
                logger.warning('Row for {} did not have the expected 3 or 4 columns'.format(tds[0].string))
                continue
        return region_names

    def _create_rs_range_list(self):
        self.rs_ranges = []
        region_servers = self._get_region_servers()
        for region_server in region_servers:
            region_ranges = self._get_region_ranges(region_server)
            for name, start, stop in region_ranges:
                start_hexstring = '0'
                stop_hexstring = 'Z'
                if start is None:
                    logger.info('Start key for {} in server {} was blank'.format(name, region_server))
                else:
                    if not isinstance(start, str):
                        start = start.decode('string-escape')
                    else:
                        start = start.encode('utf-8').decode('unicode-escape')
                    start_hexstring = self._dirtystring_to_rowkey(start)

                if stop is None:
                    logger.info('End key for {} in server {} was blank'.format(name, region_server))
                else:
                    if not isinstance(stop, str):
                        stop = stop.decode('string-escape')
                    else:
                        stop = stop.encode('utf-8').decode('unicode-escape')
                    stop_hexstring = self._dirtystring_to_rowkey(stop)

                self.rs_ranges.append([
                    region_server,
                    name,
                    start_hexstring,
                    stop_hexstring
                ])
        # Sort this by stop key
        self.rs_ranges = sorted(self.rs_ranges, key=lambda info: info[3])
        self._flush_ranges_to_file()
        return self.rs_ranges

    # The HBase UI displays keys as "\x"-prefixed hex bytes alongside ASCII characters (if the underlying hex byte can convert to an ASCII char),
    #   eg. "\x00\x12M\xCEW" (M and W are converted ASCII here)
    # This method converts the key string into a full hexstring eg. "00124DCE57" for the above example
    # FIXME: this is not tolerant of an actual ASCII "\" followed by an ASCII "x", eg. "5C78" in hex, appearing in the HTML string
    def _dirtystring_to_rowkey(self, dirty_string):
        return ''.join(map(self._char_to_hexbyte, list(dirty_string)))

    def _char_to_hexbyte(self, char):
        return '{:02X}'.format(ord(char))
