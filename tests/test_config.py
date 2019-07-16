""" Unit test for configuration file/data.
    To execute on a command line, run:
    python -m unittest tests.test_config

"""

import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import sys
import os
from genben import cli, config


class TestConfigurationFile(unittest.TestCase):

    def test_reading_runtime_configuration(self):
        """ Tests that we can read values from the benchmark.conf and into a proper data structure. """
        testargs = ["prog", "exec", "--config_file", "tests/data/config_test_reading_runtime.conf"]
        with patch.object(sys, "argv", testargs):
            args = cli.get_cli_arguments()
            runtime_configuration = config.read_configuration(location=args["config_file"])
            self.assertEqual(runtime_configuration.output["output_results"], "~/benchmark/results.psv",
                             "Runtime configuration did not have the right output file.")

    def test_generate_default_config(self):
        location = "./test_generate_default_config.conf"
        location_expected = "./genben/config/benchmark.conf.default"

        # Remove any existing configuration files from previous unit testing (prevent false positive)
        if os.path.isfile(location):
            os.remove(location)

        # Generate the default config file
        config.generate_default_config_file(output_location=location, overwrite=False)

        if os.path.isfile(location) and os.path.isfile(location_expected):
            # Read generated config file contents
            with open(location) as file:
                data_generated = file.readlines()

            with open(location_expected) as file:
                data_expected = file.readlines()

            # Check contents of default config file
            if data_generated == [] or data_expected == []:
                self.fail("Default configuration file contents was empty. Could not successfully test.")

            self.assertEqual("".join(data_generated), "".join(data_expected))
        else:
            self.fail(msg="Default configuration file was not generated or could not be found on filesystem.")

        # Cleanup test config file
        if os.path.isfile(location):
            os.remove(location)

    def test_generate_default_config_no_overwrite(self):
        location = "./test_generate_default_config_no_overwrite.conf"
        location_default = "./genben/config/benchmark.conf.default"
        test_string = "Test data"

        # Remove any existing configuration files from previous unit testing (prevent false positive)
        if os.path.isfile(location):
            os.remove(location)

        # Generate a text file that should not be overwritten
        with open(location, "w+") as file:
            file.write(test_string)

        # Generate the default config file, not overwriting any existing file
        config.generate_default_config_file(output_location=location, overwrite=False)

        if os.path.isfile(location) and os.path.isfile(location_default):
            # Read generated config file contents
            with open(location) as file:
                data_generated = file.readlines()

            with open(location_default) as file:
                data_default = file.readlines()

            # Check contents of default config file
            if data_generated == [] or data_default == []:
                self.fail("Default configuration file contents was empty. Could not successfully test.")

            self.assertEqual("".join(data_generated), test_string)
            self.assertNotEqual("".join(data_generated), "".join(data_default))
        else:
            self.fail(msg="Default configuration file was not generated or could not be found on filesystem.")

        # Cleanup test config file
        if os.path.isfile(location):
            os.remove(location)

    def test_generate_default_config_force(self):
        location = "./test_generate_default_config_force.conf"
        location_expected = "./genben/config/benchmark.conf.default"
        test_string = "Test data"

        # Remove any existing configuration files from previous unit testing (prevent false positive)
        if os.path.isfile(location):
            os.remove(location)

        # Generate a text file that should be overwritten
        with open(location, "w+") as file:
            file.write(test_string)

        # Generate the default config file, overwriting any existing file
        config.generate_default_config_file(output_location=location, overwrite=True)

        if os.path.isfile(location) and os.path.isfile(location_expected):
            # Read generated config file contents
            with open(location) as file:
                data_generated = file.readlines()

            # Read default config file contents
            with open(location_expected) as file:
                data_expected = file.readlines()

            # Check contents of default config file
            if data_generated == [] or data_expected == []:
                self.fail("Default configuration file contents was empty. Could not successfully test.")

            self.assertEqual("".join(data_generated), "".join(data_expected))
        else:
            self.fail(msg="Default configuration file was not generated or could not be found on filesystem.")

        # Cleanup test config file
        if os.path.isfile(location):
            os.remove(location)


if __name__ == "__main__":
    unittest.main()
