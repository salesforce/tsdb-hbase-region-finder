'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

import pytest
import time
from regionfinder import TSDBClient, error

class MockTSDBResponse:
    def __init__(self, json_data):
        self.json_data = json_data

    def json(self):
        return self.json_data

fake_tsuid = '000000000000BBBBBBCCCCCC'
salted_tsuid = '1222222AAAAAABBBBBB'
def get_side_effect(arg):
    if 'dict' in arg:
        return MockTSDBResponse(dict())
    if 'empty_arr' in arg:
        return MockTSDBResponse([])
    if 'empty_dps' in arg:
        return MockTSDBResponse({'dps':{}, 'tsuids': [fake_tsuid]})
    if 'salted' in arg:
        return MockTSDBResponse([{'dps':{'1514764800': '1.0'}, 'tsuids': [salted_tsuid]}])
    return MockTSDBResponse([{'dps':{'1514764800': '1.0'}, 'tsuids': [fake_tsuid]}])

class TestTSDBClient:
    def test_get_encoded_timestamp(self):
        client = TSDBClient('', 3, 0)
        assert client._get_encoded_timestamp(0) == ('0'*8)
        assert client._get_encoded_timestamp(4095) == ('0'*5 + 'F'*3)
        assert len(client._get_encoded_timestamp(time.time())) == 8

    def test_get_rowkeys_of(self, mocker):
        mocker.patch('regionfinder.tsdb_client.requests.get', side_effect=get_side_effect)
        client = TSDBClient('', 6, 0)
        with pytest.raises(error.RegionFinderError):
            client.get_rowkeys_of('dict')
        with pytest.raises(error.RegionFinderError):
            client.get_rowkeys_of('empty_arr')
        with pytest.raises(error.RegionFinderError):
            client.get_rowkeys_of('empty_dps')
        assert len(client.get_rowkeys_of('normal')[0]) == (len(fake_tsuid) + 8)

        client = TSDBClient('', 3, 1)
        assert len(client.get_rowkeys_of('salted')[0]) == (len(salted_tsuid) + 8)
