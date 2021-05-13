import unittest
from process import *


class ParseCSVTest(unittest.TestCase):

    def test_csv_read_data_headers(self):
        self.assertEqual(
            parse_csv(),
            ['', 'H', 'Customer_Name', 'Customer_Id', 'Open_Date', 'Last_Consulted_Date', 'Vaccination_Id', 'Dr_Name',
             'State', 'Country', 'DOB', 'Is_Active'])


if __name__ == '__main__':
    unittest.main()
