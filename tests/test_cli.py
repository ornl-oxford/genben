""" Unit test for benchmark CLI functions. 
    To execute on a command line, run:  
    python -m unittest tests.test_cli 

"""
import unittest

import sys

try:
    # python 3.4+ should use builtin unittest.mock not mock package
    from unittest.mock import patch
except ImportError:
    from mock import patch

from benchmark import cli

class TestStringMethods(unittest.TestCase):

    def test_getting_arguments(self):
        """ Look for default values for config """
        testargs = ["prog", "--fetch_data", "--label", "my_run"]
        with patch.object(sys, 'argv', testargs):
        #Test here if it is getting all the right things
            args = cli.get_cli_arguments()
            self.assertEqual(args.config,"../doc/benchmark.conf")
            self.assertTrue(args.fetch_data)
            self.assertEqual(args.label,"my_run")   

        testargs = ["prog"]
        with patch.object(sys, 'argv', testargs):
        #Test here if it is fetching all the things by default
            args = cli.get_cli_arguments()
            self.assertEqual(args.config,"../doc/benchmark.conf")
            self.assertFalse(args.fetch_data)
            self.assertEqual(args.label,"run")   

    def test_wrong_argument(self):
        """ test that parsing fails on a wrong argument name """
        testargs = ["prog","something"]
        with patch.object(sys, 'argv', testargs):
            try:
                cli.get_cli_arguments()
            except SystemExit:
                pass
            else:
                self.fail("It was supposed to fail on the wrong argument (something)")

    def test_parsing_arguments(self):
        testargs = ["prog", "--fetch_data", "--label", "run","--config","doc/benchmark.conf"]
        with patch.object(sys, 'argv', testargs):
            args = cli.get_cli_arguments()
            runtime_configuration = cli.parse_arguments(args)
            self.assertEqual(runtime_configuration["label"],"run", "Should have had run in it.")
            self.assertTrue(runtime_configuration["fetch_data"],"run", "Should have had run in it.")
            self.assertExists(runtime_configuration[""])


    def test_reading_runtime_configuration(self):
        """ Tests that we can read values from the benchmark.conf and into a proper data structure. """
        testargs = ["prog"]
        with patch.object(sys, 'argv', testargs):
            args = cli.get_cli_arguments()
            runtime_configuration = cli.parse_arguments(args)
            runtime_configuration.update( cli.read_configuration(location=runtime_configuration["config"]) )
            self.assertTrue(runtime_configuration["output_results"] is not None, "Should have output configuration in it.")

if __name__ == '__main__':
    unittest.main()