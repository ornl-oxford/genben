""" Unit test for benchmark CLI functions. 
    To execute on a command line, run:  
    python -m unittest tests.test_cli 

"""
import unittest
import sys

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
from genben import cli


class TestCommandLineInterface(unittest.TestCase):

    def run_subparser_test(self, subparser_cmd, parameter, expected, default_key=None, default_value=None):
        """ Tests subparsers for missing arguments and default values. """
        testargs = ["prog", subparser_cmd, "--" + parameter, expected]
        with patch.object(sys, 'argv', testargs):
            args = cli.get_cli_arguments()
            self.assertEqual(args[parameter], expected,
                             subparser_cmd + " subparser did not parse right config file arg.")
            self.assertEqual(args["command"], subparser_cmd, subparser_cmd + " command was not interpreted properly")
            if default_key:
                self.assertEqual(args[default_key], default_value,
                                 subparser_cmd + " command parser did not setup the right default key " + default_key +
                                 " to " + default_value)

    def test_getting_command_arguments(self):
        """ Tests for reading args and storing values for running all benchmark options from the command line."""
        # Test group 1 -- config
        self.run_subparser_test("config", "output_config", "./benchmark.conf")
        # Test group 2 -- setup
        self.run_subparser_test("setup", "config_file", "./benhcmark.conf")
        # Test group 3 - Tests if it the argparser is setting default values """
        self.run_subparser_test("exec", "config_file", "./benchmark.conf")

    def test_parser_expected_failing(self):
        """ Test that parsing fails on no command option (a choice of a subparser), or an unrecognized command ("something") """
        testargs = ["prog"]
        command_line_error_code = 2
        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit) as cm:
                cli.get_cli_arguments()
                self.assertEqual(cm.exception.code, command_line_error_code,
                                 "CLI handler was supposed to fail on the missing command line argument.")

        testargs = ["prog", "something"]
        command_line_error_code = 2
        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit) as cm:
                cli.get_cli_arguments()
                self.assertEqual(cm.exception.code, command_line_error_code,
                                 "CLI handler was supposed to fail on the wrong command line argument.")


if __name__ == '__main__':
    unittest.main()
