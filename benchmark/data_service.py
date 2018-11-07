""" Main module for the benchmark. It reads the command line arguments, reads the benchmark configuration, 
determines the runtime mode (dynamic vs. static); if dynamic, gets the benchmark data from the server,
runs the benchmarks, and records the timer results. """

import urllib.request
from ftplib import FTP, FTP_TLS, error_perm
import time  # for benchmark timer
import csv  # for writing results
import logging
import os.path
import pathlib
import allel
import sys
import functools
import numpy as np
import zarr
import numcodecs
from numcodecs import Blosc, LZ4, LZMA
from benchmark import config

import gzip
import shutil


def create_directory_tree(path):
    """
    Creates directories for the path specified.
    :param path: The path to create dirs/subdirs for
    :type path: str
    """
    path = str(path)  # Ensure path is in str format
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def remove_directory_tree(path):
    """
    Removes the directory and all subdirectories/files within the path specified.
    :param path: The path to the directory to remove
    :type path: str
    """

    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


def fetch_data_via_ftp(ftp_config, local_directory):
    """ Get benchmarking data from a remote ftp server. 
    :type ftp_config: config.FTPConfigurationRepresentation
    :type local_directory: str
    """
    if ftp_config.enabled:
        # Create local directory tree if it does not exist
        create_directory_tree(local_directory)

        # Login to FTP server
        if ftp_config.use_tls:
            ftp = FTP_TLS(ftp_config.server)
            ftp.login(ftp_config.username, ftp_config.password)
            ftp.prot_p()  # Request secure data connection for file retrieval
        else:
            ftp = FTP(ftp_config.server)
            ftp.login(ftp_config.username, ftp_config.password)

        if not ftp_config.files:  # Auto-download all files in directory
            fetch_data_via_ftp_recursive(ftp=ftp,
                                         local_directory=local_directory,
                                         remote_directory=ftp_config.directory)
        else:
            ftp.cwd(ftp_config.directory)

            file_counter = 1
            file_list_total = len(ftp_config.files)

            for remote_filename in ftp_config.files:
                local_filename = remote_filename
                filepath = os.path.join(local_directory, local_filename)
                if not os.path.exists(filepath):
                    with open(filepath, "wb") as local_file:
                        try:
                            ftp.retrbinary('RETR %s' % remote_filename, local_file.write)
                            print("[Setup][FTP] ({}/{}) File downloaded: {}".format(file_counter, file_list_total,
                                                                                    filepath))
                        except error_perm:
                            # Error downloading file. Display error message and delete local file
                            print("[Setup][FTP] ({}/{}) Error downloading file. Skipping: {}".format(file_counter,
                                                                                                     file_list_total,
                                                                                                     filepath))
                            local_file.close()
                            os.remove(filepath)
                else:
                    print("[Setup][FTP] ({}/{}) File already exists. Skipping: {}".format(file_counter, file_list_total,
                                                                                          filepath))
                file_counter = file_counter + 1
        # Close FTP connection
        ftp.close()


def fetch_data_via_ftp_recursive(ftp, local_directory, remote_directory, remote_subdirs_list=None):
    """
    Recursive function that automatically downloads all files with a FTP directory, including subdirectories.
    :type ftp: ftplib.FTP
    :type local_directory: str
    :type remote_directory: str
    :type remote_subdirs_list: list
    """

    if (remote_subdirs_list is not None) and (len(remote_subdirs_list) > 0):
        remote_path_relative = "/".join(remote_subdirs_list)
        remote_path_absolute = "/" + remote_directory + "/" + remote_path_relative + "/"
    else:
        remote_subdirs_list = []
        remote_path_relative = ""
        remote_path_absolute = "/" + remote_directory + "/"

    try:
        local_path = local_directory + "/" + remote_path_relative
        os.mkdir(local_path)
        print("[Setup][FTP] Created local folder: {}".format(local_path))
    except OSError:  # Folder already exists at destination. Do nothing.
        pass
    except error_perm:  # Invalid Entry
        print("[Setup][FTP] Error: Could not change to: {}".format(remote_path_absolute))

    ftp.cwd(remote_path_absolute)

    # Get list of remote files/folders in current directory
    file_list = ftp.nlst()

    file_counter = 1
    file_list_total = len(file_list)

    for file in file_list:
        file_path_local = local_directory + "/" + remote_path_relative + "/" + file
        if not os.path.isfile(file_path_local):
            try:
                # Determine if a file or folder
                ftp.cwd(remote_path_absolute + file)
                # Path is for a folder. Run recursive function in new folder
                print("[Setup][FTP] Switching to directory: {}".format(remote_path_relative + "/" + file))
                new_remote_subdirs_list = remote_subdirs_list.copy()
                new_remote_subdirs_list.append(file)
                fetch_data_via_ftp_recursive(ftp=ftp, local_directory=local_directory,
                                             remote_directory=remote_directory,
                                             remote_subdirs_list=new_remote_subdirs_list)
                # Return up one level since we are using recursion
                ftp.cwd(remote_path_absolute)
            except error_perm:
                # file is an actual file. Download if it doesn't already exist on filesystem.
                temp = ftp.nlst()
                if not os.path.isfile(file_path_local):
                    with open(file_path_local, "wb") as local_file:
                        ftp.retrbinary('RETR {}'.format(file), local_file.write)
                    print("[Setup][FTP] ({}/{}) File downloaded: {}".format(file_counter, file_list_total,
                                                                            file_path_local))
        else:
            print("[Setup][FTP] ({}/{}) File already exists. Skipping: {}".format(file_counter, file_list_total,
                                                                                  file_path_local))
        file_counter = file_counter + 1


def fetch_file_from_url(url, local_file):
    urllib.request.urlretrieve(url, local_file)


def decompress_gzip(local_file_gz, local_file):
    with open(local_file, 'wb') as file_out, gzip.open(local_file_gz, 'rb') as file_in:
        shutil.copyfileobj(file_in, file_out)


def process_data_files(input_dir, temp_dir, output_dir):
    """
    Iterates through all files in input_dir and processes *.vcf.gz files to *.vcf, placed in output_dir.
    Additionally moves *.vcf files to output_dir
    Note: This method searches through all subdirectories within input_dir, and files are placed in root of output_dir.
    :param input_dir: The input directory containing files to process
    :param temp_dir: The temporary directory for unzipping *.gz files, etc.
    :param output_dir: The output directory where processed *.vcf files should go
    :type input_dir: str
    :type temp_dir: str
    :type output_dir: str
    """

    # Ensure input, temp, and output directory paths are in str format, not pathlib
    input_dir = str(input_dir)
    temp_dir = str(temp_dir)
    output_dir = str(output_dir)

    # Create input, temp, and output directories if they do not exist
    create_directory_tree(input_dir)
    create_directory_tree(temp_dir)
    create_directory_tree(output_dir)

    # Iterate through all *.gz files in input directory and uncompress them to the temporary directory
    pathlist_gz = pathlib.Path(input_dir).glob("**/*.gz")
    for path in pathlist_gz:
        path_str = str(path)
        file_output_str = path_leaf(path_str)
        file_output_str = file_output_str[0:len(file_output_str) - 3]  # Truncate *.gz from input filename
        path_temp_output = str(pathlib.Path(temp_dir, file_output_str))
        print("[Setup][Data] Decompressing file: {}".format(path_str))
        print("  - Output: {}".format(path_temp_output))

        # Decompress the .gz file
        decompress_gzip(path_str, path_temp_output)

    # Iterate through all files in temporary directory and move *.vcf files to output directory
    pathlist_vcf_temp = pathlib.Path(temp_dir).glob("**/*.vcf")
    for path in pathlist_vcf_temp:
        path_temp_str = str(path)
        filename_str = path_leaf(path_temp_str)  # Strip filename from path
        path_vcf_str = str(pathlib.Path(output_dir, filename_str))

        shutil.move(path_temp_str, path_vcf_str)

    # Remove temporary directory
    remove_directory_tree(temp_dir)

    # Copy any *.vcf files already in input directory to the output directory
    pathlist_vcf_input = pathlib.Path(input_dir).glob("**/*.vcf")
    for path in pathlist_vcf_input:
        path_input_str = str(path)
        filename_str = path_leaf(path_input_str)  # Strip filename from path
        path_vcf_str = str(pathlib.Path(output_dir, filename_str))

        shutil.copy(path_input_str, path_vcf_str)


def path_head(path):
    head, tail = os.path.split(path)
    return head


def path_leaf(path):
    head, tail = os.path.split(path)
    return tail or os.path.basename(head)


def read_file_contents(local_filepath):
    if os.path.isfile(local_filepath):
        with open(local_filepath) as f:
            data = f.read()
            return data
    else:
        return None


def setup_vcf_to_zarr(input_vcf_dir, output_zarr_dir, conversion_config):
    """
    Converts all VCF files in input directory to Zarr format, placed in output directory,
    based on conversion configuration parameters
    :param input_vcf_dir: The input directory where VCF files are located
    :param output_zarr_dir: The output directory to place Zarr-formatted data
    :param conversion_config: Configuration data for the conversion
    :type input_vcf_dir: str
    :type output_zarr_dir: str
    :type conversion_config: config.VCFtoZarrConfigurationRepresentation
    """
    # Ensure input and output directory paths are in str format, not pathlib
    input_vcf_dir = str(input_vcf_dir)
    output_zarr_dir = str(output_zarr_dir)

    # Create input and output directories if they do not exist
    create_directory_tree(input_vcf_dir)
    create_directory_tree(output_zarr_dir)

    # Iterate through all *.vcf files in input directory and convert to Zarr format
    pathlist_vcf = pathlib.Path(input_vcf_dir).glob("**/*.vcf")
    for path in pathlist_vcf:
        path_str = str(path)
        file_output_str = path_leaf(path_str)
        file_output_str = file_output_str[0:len(file_output_str) - 4]  # Truncate *.vcf from input filename
        path_zarr_output = str(pathlib.Path(output_zarr_dir, file_output_str))
        print("[Setup][Data] Converting VCF file to Zarr format: {}".format(path_str))
        print("  - Output: {}".format(path_zarr_output))

        # Convert to Zarr format
        convert_to_zarr(input_vcf_path=path_str,
                        output_zarr_path=path_zarr_output,
                        conversion_config=conversion_config)


def convert_to_zarr(input_vcf_path, output_zarr_path, conversion_config):
    """ Converts the original data (VCF) to a Zarr format. Only converts a single VCF file.
    :param input_vcf_path: The input VCF file location
    :param output_zarr_path: The desired Zarr output location
    :param conversion_config: Configuration data for the conversion
    :type input_vcf_path: str
    :type output_zarr_path: str
    :type conversion_config: config.VCFtoZarrConfigurationRepresentation
    """
    if conversion_config is not None:
        # Ensure var is string, not pathlib.Path
        output_zarr_path = str(output_zarr_path)

        # Get alt number
        if conversion_config.alt_number is None:
            print("[VCF-Zarr] Determining maximum number of ALT alleles by scaling all variants in the VCF file.")
            # Scan VCF file to find max number of alleles in any variant
            callset = allel.read_vcf(input_vcf_path, fields=['numalt'], log=sys.stdout)
            numalt = callset['variants/numalt']
            alt_number = np.max(numalt)
        else:
            print("[VCF-Zarr] Using alt number provided in configuration.")
            # Use the configuration-provided alt number
            alt_number = conversion_config.alt_number
        print("[VCF-Zarr] Alt number: {}".format(alt_number))

        # Get chunk length
        chunk_length = allel.vcf_read.DEFAULT_CHUNK_LENGTH
        if conversion_config.chunk_length is not None:
            chunk_length = conversion_config.chunk_length
        print("[VCF-Zarr] Chunk length: {}".format(chunk_length))

        # Get chunk width
        chunk_width = allel.vcf_read.DEFAULT_CHUNK_WIDTH
        if conversion_config.chunk_width is not None:
            chunk_width = conversion_config.chunk_width
        print("[VCF-Zarr] Chunk width: {}".format(chunk_width))

        if conversion_config.compressor == "Blosc":
            compressor = Blosc(cname=conversion_config.blosc_compression_algorithm,
                               clevel=conversion_config.blosc_compression_level,
                               shuffle=conversion_config.blosc_shuffle_mode)
        else:
            raise ValueError("Unexpected compressor type specified.")

        print("[VCF-Zarr] Using {} compressor.".format(conversion_config.compressor))

        print("[VCF-Zarr] Performing VCF to Zarr conversion...")
        # Perform the VCF to Zarr conversion
        allel.vcf_to_zarr(input_vcf_path, output_zarr_path, alt_number=alt_number, overwrite=True,
                          log=sys.stdout, compressor=compressor, chunk_length=chunk_length, chunk_width=chunk_width)
        print("[VCF-Zarr] Done.")
