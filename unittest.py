import unittest
from main import wildcard_match

class TestWildcardMatch(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(wildcard_match('ENTITY123', 'ENTITY123'))
    
    def test_wildcard_match(self):
        self.assertTrue(wildcard_match('ENTITY123', 'ENTITY*'))
        self.assertTrue(wildcard_match('ENTITY123', 'ENT*123'))
        self.assertTrue(wildcard_match('ENTITY123', '*123'))
    
    def test_no_match(self):
        self.assertFalse(wildcard_match('ENTITY123', 'ENTITY124'))
        self.assertFalse(wildcard_match('ENTITY123', 'ENT*124'))
        self.assertFalse(wildcard_match('ENTITY123', '*124'))
    
    def test_edge_cases(self):
        self.assertFalse(wildcard_match('', 'ENTITY123'))
        self.assertFalse(wildcard_match('ENTITY123', ''))
        self.assertTrue(wildcard_match('', ''))
        self.assertTrue(wildcard_match('ENTITY123', 'ENTITY*123'))
        self.assertFalse(wildcard_match('ENTITY123', 'ENTITY*124'))

if __name__ == '__main__':
    unittest.main()