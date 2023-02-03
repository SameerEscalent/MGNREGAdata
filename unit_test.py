# -*- coding: utf-8 -*-
"""
Created on Fri Feb  3 13:12:42 2023

"""
import unittest
from mnrega_data_preprocessing import MNREGADataLoader

class TestPreprocess(unittest.TestCase):
    
    global data
    loader = MNREGADataLoader()
    loader.download()
    data = loader.process()
        
    def test_data_headers(self):
        self.assertEqual(len(data.columns), 8)
        
    def test_data_states(self):
        self.assertGreaterEqual(len(list(data['state'].unique())), 34)
        
    def test_data_spend(self):
        self.assertGreaterEqual(data['total_actual_exp'].sum(), 0)
        
if __name__ == '__main__':
    unittest.main()