import json
import os
import unittest

import pandas as pd

from src import utils as u


class TestUtilsMethods(unittest.TestCase):
    
    def test_copy_data(self):
        u.copy_data_to_temp("test_file.csv", "./data_test/", './temp/')
        self.assertTrue(os.path.exists("./temp/test_file.csv"))
        u.copy_data_to_temp("test_file.csv", "./data_test/", './output_test/')
        os.remove('./output_test/test_file.csv')
        os.remove('./temp/test_file.csv')
        os.rmdir('./temp/')
        
    def test_load_data_file(self):
        expected_df = pd.DataFrame({'month': [1, 4], 'year': [2012, 2014], 'sale': [55, 40]})
        pd.testing.assert_frame_equal(u.load_data_file("data_test/data_test.json"), expected_df)
        pd.testing.assert_frame_equal(u.load_data_file("./data_test/broken_json.json"), expected_df)
        self.assertIsNone(u.load_data_file("data_test/data_test.parquet"))
            
    def test_write_to_json(self):
        data_df = pd.DataFrame({'month': [1, 4],
                                    'year': [2012, 2014],
                                    'sale': [55, 40]})
        u.write_to_json(data_df, './output_test/writen_json.json')
        expected_json = [{'month': 1, 'sale': 55, 'year': 2012}, {'month': 4, 'sale': 40, 'year': 2014}]
        with open('./output_test/writen_json.json') as out_file:
            actual = json.load(out_file)
        self.assertEqual(actual['data'], expected_json)
        os.remove('./output_test/writen_json.json')
