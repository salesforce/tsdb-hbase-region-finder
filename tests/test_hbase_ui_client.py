'''
  Copyright (c) 2019, salesforce.com, inc.
  All rights reserved.
  SPDX-License-Identifier: BSD-3-Clause
  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause

'''

import pytest
import os
from time import sleep
from regionfinder import HBaseUIClient

class MockHBaseUIResponse:
    master_status_html = r"""
        <div class="tab-pane active" id="tab_baseStats">
            <table class="table table-striped">
            <tr><th>ServerName</th><th>Start time</th><th>Requests Per Second</th><th>Num. Regions</th></tr>
            <tr><td><a href="//localhost:60030/">localhost,60020,1544258234698</a></td><td>Sat Dec 08 08:37:14 GMT 2018</td><td>10000</td><td>200</td></tr>
            <tr><td>Total:1</td><td></td><td>10000</td><td>200</td></tr>
            </table>
        </div>
        """
    rs_status_html = r"""
        <div class="tab-pane active" id="tab_regionBaseInfo">
            <table class="table table-striped">
            <tr><th>Region Name</th><th>Start Key</th><th>End Key</th></tr>
            <tr><td>tsdb,time.test1</td>
                <td>\x00\x00</td>
                <td>ee</td></tr>
            <tr><td>tsdb,time.test2</td>
                <td>ee</td>
                <td>\xAA\xAA</td></tr>
            <tr><td>tsdb,time.test3</td>
                <td>\xAA\xAA</td>
                <td>\xFF\xFF</td></tr>
            </table>
        </div>
        """

    def __init__(self, text):
        self.text = text

def get_side_effect(arg):
    if 'master-status' in arg:
        return MockHBaseUIResponse(MockHBaseUIResponse.master_status_html)
    return MockHBaseUIResponse(MockHBaseUIResponse.rs_status_html)

class TestHBaseUIClient:
    @pytest.fixture
    def client(self, mocker):
        mocker.patch.object(HBaseUIClient, '_load_ranges_from_file', return_value=[])
        mocker.patch('regionfinder.tsdb_client.requests.get', side_effect=get_side_effect)
        return HBaseUIClient('', 'tsdb', autorefresh=False)

    def test_dirtystring_to_rowkey(self, client):
        dirty_and_clean_pairs = [
            (u'x\x00\\','78005C'),
            (u'\\\x00x','5C0078'),
            (u'AA\x00\x00','41410000'),
            (u'\x00\x00\x00\x00','00000000'),
            (u'\x00AA\x00','00414100'),
            (u'\x00\x00AA','00004141'),
            (u'A\x00\x00A','41000041'),
        ]
        for dirty, clean in dirty_and_clean_pairs:
            assert client._dirtystring_to_rowkey(dirty) == clean

    def test_get_region_server(self, client):
        assert client._get_region_servers() == ['http://localhost:60030/']

    def test_get_and_query_region_ranges(self, mocker, client):
        client._get_region_servers = mocker.MagicMock(return_value=[''])
        client._flush_ranges_to_file = mocker.MagicMock(return_value=None)
        assert client._get_region_ranges('') == [
            ['tsdb,time.test1', r'\x00\x00', r'ee'],
            ['tsdb,time.test2', r'ee', r'\xAA\xAA'],
            ['tsdb,time.test3', r'\xAA\xAA', r'\xFF\xFF']]
        client._create_rs_range_list()
        assert ('', 'tsdb,time.test1') == client.get_rs_of_rowkey('3333')
        assert ('', 'tsdb,time.test2') == client.get_rs_of_rowkey('A1A1')
        assert ('', 'tsdb,time.test3') == client.get_rs_of_rowkey('BBBB')

    def test_load_and_flush_cache(self, mocker):
        rs_ranges = [
            ['rs1', 'r1', '00', '88'],
            ['rs2', 'r1', '88', 'FF']
        ]
        HBaseUIClient.CACHE_FILENAME = mocker.PropertyMock(return_value='rf-test.csv')
        load_function = HBaseUIClient._load_ranges_from_file
        # Patch the load function so it doesn't try to populate info in the client __init__
        mocker.patch.object(HBaseUIClient, '_load_ranges_from_file', return_value=[])
        client = HBaseUIClient('', 'tsdb', autorefresh=False)
        client.rs_ranges = rs_ranges[:]
        client._flush_ranges_to_file()
        # Unpatch the load function so that a new client will load from the previously flushed cache file
        HBaseUIClient._load_ranges_from_file = load_function
        client2 = HBaseUIClient('', 'tsdb', autorefresh=False)
        assert client2.rs_ranges == rs_ranges
        os.remove(HBaseUIClient.CACHE_FILENAME)

    def test_autorefresh(self, mocker):
        mocker.patch.object(HBaseUIClient, '_load_ranges_from_file', return_value=[])
        HBaseUIClient.EXPIRY_SECONDS = 1
        client = HBaseUIClient('', 'tsdb', autorefresh=True)
        client._create_rs_range_list = mocker.MagicMock(return_value=[])
        sleep(3)
        assert client._create_rs_range_list.call_count >= 2
        client.active_timer.cancel()
