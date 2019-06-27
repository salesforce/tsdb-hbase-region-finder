'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

import logging
import sys
import os

logger = logging.getLogger('regionfinder')
logger.setLevel(logging.INFO)
file_logger = logging.FileHandler(os.environ.get('REGION_FINDER_LOG', 'rf.log'))
file_logger.setLevel(logging.INFO)
stream_logger = logging.StreamHandler()
stream_logger.setLevel(logging.WARNING)
logging_formatter = logging.Formatter('[ %(asctime)s | %(name)s | %(levelname)s ] %(message)s')
file_logger.setFormatter(logging_formatter)
stream_logger.setFormatter(logging_formatter)
logger.addHandler(file_logger)
logger.addHandler(stream_logger)

if sys.version_info[0] == 3:
    binary_type = lambda s: bytes(s, 'UTF-8')
    open_csv_r = lambda f: open(f, newline='')
    open_csv_w = lambda f: open(f, 'w', newline='')
else:
    binary_type = str
    open_csv_r = lambda f: open(f, 'rb')
    open_csv_w = lambda f: open(f, 'wb')

PROJECT_NAME = 'tsdb-hbase-region-finder'

def default_config_path():
    return os.environ['HOME'] + '/.conf/' + PROJECT_NAME + '/config.yaml'
