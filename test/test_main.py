import os
import unittest

import pandas as pd

from src import main as u


class TestUtilsMethods(unittest.TestCase):

    def test_preprocess_data(self):
        pubmed_df = pd.DataFrame({"title": ["Some WORDS"], "scientific_title": ["OTHER words"]})
        pd.testing.assert_frame_equal(u.preprocess_data(pubmed_df, "pubmed"),
                                      pd.DataFrame({"title": ["some words"], "scientific_title": ["OTHER words"]}))
        trials_df = pd.DataFrame({"title": ["Some WORDS"], "scientific_title": ["OTHER words"]})
        pd.testing.assert_frame_equal(u.preprocess_data(trials_df, "clinical_trials"),
                                      pd.DataFrame({"title": ["Some WORDS"], "scientific_title": ["other words"]}))
        drugs_df = pd.DataFrame({"atccode": ["A04AD"], "drug": ["DIPHENHYDRAMINE"]})
        pd.testing.assert_frame_equal(u.preprocess_data(drugs_df, "drugs"),drugs_df)

    def test_data_loader(self):
        df = pd.DataFrame({"month": {0: 1, 1: 4, 2: 1, 3: 4}, 
                           "year": {0: 2012, 1: 2014, 2: 2012, 3: 2014}, 
                           "sale": {0: 55, 1: 40, 2: 55, 3: 40}})
        pd.testing.assert_frame_equal(df, u.load_all_data("./", "data_test"))
        
    def test_pipeline(self):
        u.main_compute("../data/", "../test/output_test/out.json")
        
        # asserting one single ouput file
        self.assertEqual(os.listdir("../test/output_test/"), ["out.json"])
        
        # asserting that the model is respected 
        df = pd.read_json("../test/output_test/out.json", orient="table")
        self.assertEqual(list(df.columns), ["drug", "type", "date", "value"])
        
        os.remove("../test/output_test/out.json")
