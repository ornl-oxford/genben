""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import argparse  # for command line parsing
import datetime
import time  # for benchmark timer
import csv  # for writing results
import logging
import sys
import shutil
from genben import core, config, data_service, dask_utils


def get_cli_arguments():
    """ Returns command line arguments. 

    Returns:
    args object from an ArgumentParses for fetch data (boolean, from a server), label (optional, for naming the benchmark run), 
    and config argument for where is the config file. """

    logging.debug('Getting cli arguments')

    parser = argparse.ArgumentParser(description="A benchmark for genomics routines in Python.")

    # Enable three exclusive groups of options (using subparsers)
    # https://stackoverflow.com/questions/17909294/python-argparse-mutual-exclusive-group/17909525

    subparser = parser.add_subparsers(title="commands", dest="command")
    subparser.required = True

    config_parser = subparser.add_parser("config",
                                         help='Setting up the default configuration of the benchmark. It creates the default configuration file.')
    config_parser.add_argument("--output_config", type=str, required=True,
                               help="Specify the output path to a configuration file.", metavar="FILEPATH")
    config_parser.add_argument("-f", action="store_true", help="Overwrite the destination file if it already exists.")

    data_setup_parser = subparser.add_parser("setup",
                                             help='Preparation and setting up of the data for the benchmark. It requires a configuration file.')
    data_setup_parser.add_argument("--config_file", required=True, help="Location of the configuration file",
                                   metavar="FILEPATH")

    benchmark_exec_parser = subparser.add_parser("exec",
                                                 help='Execution of the benchmark modes. It requires a configuration file.')

    timestamp_current = datetime.datetime.fromtimestamp(time.time())
    benchmark_label_default = "run_{timestamp}".format(timestamp=timestamp_current.strftime("%Y-%m-%d_%H-%M-%S"))
    benchmark_exec_parser.add_argument("--label", type=str, default=benchmark_label_default, metavar="RUN_LABEL",
                                       help="Label for the benchmark run.")
    benchmark_exec_parser.add_argument("--config_file", type=str, required=True,
                                       help="Specify the path to a configuration file.", metavar="FILEPATH")

    runtime_configuration = vars(parser.parse_args())
    return runtime_configuration


def _main():
    data_dirs = config.DataDirectoriesConfigurationRepresentation()

    cli_arguments = get_cli_arguments()

    command = cli_arguments["command"]
    if command == "config":
        output_config_location = cli_arguments["output_config"]
        overwrite_mode = cli_arguments["f"]
        config.generate_default_config_file(output_location=output_config_location,
                                            overwrite=overwrite_mode)
    elif command == "setup":
        print("[Setup] Setting up benchmark data.")

        # Clear out existing files in VCF and Zarr directories
        data_service.remove_directory_tree(data_dirs.vcf_dir)
        data_service.remove_directory_tree(data_dirs.zarr_dir_setup)

        # Get runtime config from specified location
        runtime_config = config.read_configuration(location=cli_arguments["config_file"])

        # Get FTP module settings from runtime config
        ftp_config = config.FTPConfigurationRepresentation(runtime_config)

        if ftp_config.enabled:
            print("[Setup][FTP] FTP module enabled. Running FTP download...")
            data_service.fetch_data_via_ftp(ftp_config=ftp_config, local_directory=data_dirs.download_dir)
        else:
            print("[Setup][FTP] FTP module disabled. Skipping FTP download...")

        # Process/Organize downloaded files
        data_service.process_data_files(input_dir=data_dirs.input_dir,
                                        temp_dir=data_dirs.temp_dir,
                                        output_dir=data_dirs.vcf_dir)

        # Convert VCF files to Zarr format if the module is enabled
        vcf_to_zarr_config = config.VCFtoZarrConfigurationRepresentation(runtime_config)
        if vcf_to_zarr_config.enabled:
            data_service.setup_vcf_to_zarr(input_vcf_dir=data_dirs.vcf_dir,
                                           output_zarr_dir=data_dirs.zarr_dir_setup,
                                           conversion_config=vcf_to_zarr_config)
    elif command == "exec":
        print("[Exec] Executing benchmark tool.")

        # Clear out existing files in Zarr benchmark directory
        data_service.remove_directory_tree(data_dirs.zarr_dir_benchmark)

        # Get runtime config from specified location
        runtime_config = config.read_configuration(location=cli_arguments["config_file"])

        # Get benchmark label
        benchmark_label = cli_arguments["label"]

        # Extract Dask scheduler configuration
        dask_config = config.DaskSchedulerConfigurationRepresentation(runtime_config)

        # Connect to Dask scheduler if enabled
        if dask_config.enabled:
            du = dask_utils.DaskUtils()
            du.connect_to_scheduler(address=dask_config.scheduler_address,
                                    port=dask_config.scheduler_port)

        # Get Benchmark module settings from runtime config
        benchmark_config = config.BenchmarkConfigurationRepresentation(runtime_config)

        # Setup the benchmark runner
        benchmark = core.Benchmark(bench_conf=benchmark_config, data_dirs=data_dirs, benchmark_label=benchmark_label)

        # Run the benchmark
        benchmark.run_benchmark()
    else:
        print("Error: Unexpected command specified. Exiting...")
        sys.exit(1)
