'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''


import yaml
import os
import sys
import errno
from regionfinder.util import default_config_path, logger

TEMPLATE = """---
tsdb:
  endpoint: http://localhost:4466
  metricWidth: 3
  saltWidth: 0
hbaseMaster:
  endpoint: http://localhost:60010
  tableName: tsdb
cacheDir: # optional, defaults to same dir as default config path
"""
class Config:
    def __init__(self, filepath):
        if filepath is None or len(filepath) == 0:
            filepath = default_config_path()

            if not os.path.isfile(filepath):
                if not os.path.exists(os.path.dirname(filepath)):
                    try:
                        os.makedirs(os.path.dirname(filepath))
                    except OSError as exc:  # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                config_file = open(filepath, 'w+')
                config_file.write(TEMPLATE)
                config_file.close()
                sys.exit('A template config file has been created at the default path: {}. Please fill in the template and retry'.format(filepath))

        if not os.path.isfile(filepath):
            sys.exit('Config file was not found at {}. Please create a config file following the template:\n\nconfig.yaml\n{}\n'.format(filepath, TEMPLATE))

        self._filepath = filepath
        with open(filepath, 'r') as file:
            self._params = yaml.load(file)
            try:
                self._assertKeyIsPresent('tsdb')
                self._assertKeyIsPresent('tsdb', 'endpoint')
                self._assertKeyIsPresent('tsdb', 'metricWidth')
                self._assertKeyIsPresent('tsdb', 'saltWidth')
                self._assertKeyIsPresent('hbaseMaster')
                self._assertKeyIsPresent('hbaseMaster', 'endpoint')
                self._assertKeyIsPresent('hbaseMaster', 'tableName')
            except AssertionError as err:
                logger.error(str(err))
                sys.exit(1)

            self.tsdb_url = self._params['tsdb']['endpoint']
            self.tsdb_metric_width = int(self._params['tsdb']['metricWidth'])
            self.tsdb_salt_width = int(self._params['tsdb']['saltWidth'])
            self.hbase_url = self._params['hbaseMaster']['endpoint']
            self.hbase_table_name = self._params['hbaseMaster']['tableName']
            if self._params.get('cacheDir') is None or len(self._params['cacheDir'].strip()) == 0:
                self.cache_dir = os.path.dirname(filepath)
            else:
                self.cache_dir = self._params['cacheDir']
            logger.info('Using {} as the cache directory'.format(self.cache_dir))

    def _assertKeyIsPresent(self, *keys):
        root = self._params
        for key in keys:
            root = root.get(key)
            assert root is not None, 'Missing this key in config YAML: {}\nin file {}'.format('.'.join(keys), self._filepath)
