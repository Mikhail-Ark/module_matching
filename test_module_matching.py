import pandas as pd
import unittest

from libs.cmodel.module_matching import auto_module_matching, module_matching, fetch_mm_data, add_info, log_mm_statistics
from libs.cmodel.smart_matching import smart_matching, get_model


class TestMethods(unittest.TestCase):

    def test_add_info(self):
        test_df = pd.DataFrame([
            {"id": "5834b933-fd63-4682-b68a-fdda63fd68b8", "x": "Амиодарон  0.2   г N30  таб. Иннолек", "ean": "4810201006314", "nomcode": "38091"},
            {"id": "111-111", "x": "Розувастин  0.04 г N30  таб. Изварино", "ean": "4810201006314", "nomcode": "000001"},
        ])
        result = add_info(test_df, add_groups=True)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn("m_group", result.columns)
        self.assertIn("ph_group", result.columns)
        self.assertIn("preds", result.columns)
        self.assertIn("m_preds", result.columns)
        self.assertTrue(result["m_group"].notna().all())
        self.assertTrue(result["ph_group"].notna().all())
        self.assertTrue(result["preds"].notna().all())
        self.assertTrue(result["m_preds"].notna().all())


    def test_log_statistics(self):
        test_res = pd.DataFrame([
            {"id": "0", "m_group": "90000", "ph_group": "0", "y": "000", "bonus": "100.5"},
            {"id": "1", "m_group": "90000", "ph_group": "1", "y": None, "bonus": None},
            {"id": "2", "m_group": "1", "ph_group": "2", "y": None, "bonus": None},
            {"id": "3", "m_group": "1", "ph_group": "3", "y": None, "bonus": None},
            {"id": "4", "m_group": "2", "ph_group": None, "y": "001", "bonus": "50.4"}
        ])
        test_train = pd.DataFrame([
            {"id": "000", "bonus": "100.5"},
            {"id": "001", "bonus": "50.4"},
            {"id": "002", "bonus": "20.3"}
        ])
        correct_answer = {
            "endpoint": "test", "n_nom": 5, "n_bon": 3, "s_bon": 171.2, "n_mg_nom": 2, "n_phg_nom": 4,
            "n_bon+": 1, "s_bon+": 100.5, "n_bon-": 1, "s_bon-": 20.3, "n_bon-gr": 1, "s_bon-gr": 50.4
        }
        answers = log_mm_statistics(test_res, test_train, "test")
        self.assertEqual(answers, correct_answer)

if __name__ == '__main__':
    unittest.main()
