'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

import os
from regionfinder import Config
from regionfinder.config import TEMPLATE

class TestConfig:
    FILEPATH = './rf-config-text.yaml'

    def test_load_config_from_template(self):
        with open(self.FILEPATH, 'w') as f:
            f.write(TEMPLATE)
        config = Config(self.FILEPATH)
        assert config.tsdb_url is not None
        assert config.tsdb_salt_width is not None
        assert config.tsdb_metric_width is not None
        assert config.hbase_url is not None
        assert config.hbase_table_name is not None
        os.remove(self.FILEPATH)
