'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

import requests
from regionfinder import error

class TSDBClient:
    PARAMS = {
        'show_tsuids': 'true'
    }
    def __init__(self, instance_url, metric_width, salt_width):
        self.instance_url = instance_url
        self.pretag_length = salt_width*2 + metric_width*2

    # Rowkey format: <metric_uid 6B><timestamp 4B><tagk1><tagv1>[...<tagkN><tagvN>]
    def get_rowkeys_of(self, metric_name, start_time='1h-ago'):
        timestamp, tsuids = self._query(metric_name, start_time)
        ets = self._get_encoded_timestamp(timestamp)
        return [uid[:self.pretag_length] + ets + uid[self.pretag_length:] for uid in tsuids]

    def _query(self, metric_name, start_time):
        qs_dict = self.PARAMS.copy()
        qs_dict['start'] = start_time
        qs_dict['m'] = 'sum:' + metric_name
        qs = '&'.join([k + '=' + qs_dict[k] for k in qs_dict])
        resp = requests.get(self.instance_url + '/api/query?' + qs)
        query_result = resp.json()
        if isinstance(query_result, dict) or len(query_result) == 0:
            raise error.RegionFinderError('No results from TSDB for metric: {}'.format(metric_name))
        metric = query_result[0]
        if metric.get('dps') is None or len(metric.get('dps')) == 0:
            raise error.RegionFinderError('No datapoints for metric {} at time {}'.format(metric, start_time))
        if metric.get('tsuids') is None or len(metric.get('tsuids')) == 0:
            raise error.RegionFinderError('No TSUIDs found in TSDB response')

        timestamp = min(metric['dps'])
        tsuids = metric['tsuids']
        return timestamp, tsuids

    # Timestamp component of HB row key is a Unix epoch value in seconds encoded on 4 bytes
    def _get_encoded_timestamp(self, ts):
        return "{0:08X}".format(int(ts))
