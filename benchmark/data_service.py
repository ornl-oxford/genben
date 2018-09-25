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
from numcodecs import Blosc
from benchmark import config

import gzip
import shutil


def fetch_data_via_ftp(ftp_config, local_directory):
    """ Get benchmarking data from a remote ftp server. 
    :type ftp_config: config.FTPConfigurationRepresentation
    :type local_directory: str
    """
    if ftp_config.enabled:
        # Create local directory tree if it does not exist
        pathlib.Path(local_directory).mkdir(parents=True, exist_ok=True)

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
		
def convert_to_zarr(source_data,zarr_formatted_data):
	""" This function converts the original data (vcf) to a zarr format. """
	pass 


def read_file_contents(local_filepath):
    if os.path.isfile(local_filepath):
        with open(local_filepath) as f:
            data = f.read()
            return data
    else:
        return None


