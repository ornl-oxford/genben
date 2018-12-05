from configparser import ConfigParser
from shutil import copyfile
import os.path
from pkg_resources import resource_string
from numcodecs import Blosc


def config_str_to_bool(input_str):
    """
    :param input_str: The input string to convert to bool value
    :type input_str: str
    :return: bool
    """
    return input_str.lower() in ['true', '1', 't', 'y', 'yes']


class DataDirectoriesConfigurationRepresentation:
    input_dir = "./data/input/"
    download_dir = input_dir + "download/"
    temp_dir = "./data/temp/"
    vcf_dir = "./data/vcf/"
    zarr_dir_setup = "./data/zarr/"
    zarr_dir_benchmark = "./data/zarr_benchmark/"


def isint(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def isfloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


class ConfigurationRepresentation(object):
    """ A small utility class for object representation of a standard config. file. """

    def __init__(self, file_name):
        """ Initializes the configuration representation with a supplied file. """
        parser = ConfigParser()
        parser.optionxform = str  # make option names case sensitive
        found = parser.read(file_name)
        if not found:
            raise ValueError("Configuration file {0} not found".format(file_name))
        for name in parser.sections():
            dict_section = {name: dict(parser.items(name))}  # create dictionary representation for section
            self.__dict__.update(dict_section)  # add section dictionary to root dictionary


class FTPConfigurationRepresentation(object):
    """ Utility class for object representation of FTP module configuration. """
    enabled = False  # Specifies whether the FTP module should be enabled or not
    server = ""  # FTP server to connect to
    username = ""  # Username to login with. Set username and password to blank for anonymous login
    password = ""  # Password to login with. Set username and password to blank for anonymous login
    use_tls = False  # Whether the connection should use TLS encryption
    directory = ""  # Directory on FTP server to download files from
    files = []  # List of files within directory to download. Set to empty list to download all files within directory

    def __init__(self, runtime_config=None):
        """
        Creates an object representation of FTP module configuration data.
        :param runtime_config: runtime_config data to extract FTP settings from
        :type runtime_config: ConfigurationRepresentation
        """
        if runtime_config is not None:
            # Check if [ftp] section exists in config
            if hasattr(runtime_config, "ftp"):
                # Extract relevant settings from config file
                if "enabled" in runtime_config.ftp:
                    self.enabled = config_str_to_bool(runtime_config.ftp["enabled"])
                if "server" in runtime_config.ftp:
                    self.server = runtime_config.ftp["server"]
                if "username" in runtime_config.ftp:
                    self.username = runtime_config.ftp["username"]
                if "password" in runtime_config.ftp:
                    self.password = runtime_config.ftp["password"]
                if "use_tls" in runtime_config.ftp:
                    self.use_tls = config_str_to_bool(runtime_config.ftp["use_tls"])
                if "directory" in runtime_config.ftp:
                    self.directory = runtime_config.ftp["directory"]

                # Convert delimited list of files (string) to Python-style list
                if "file_delimiter" in runtime_config.ftp:
                    delimiter = runtime_config.ftp["file_delimiter"]
                else:
                    delimiter = "|"

                if "files" in runtime_config.ftp:
                    files_str = str(runtime_config.ftp["files"])
                    if files_str == "*":
                        self.files = []
                    else:
                        self.files = files_str.split(delimiter)


vcf_to_zarr_compressor_types = ["Blosc"]
vcf_to_zarr_blosc_algorithm_types = ["zstd", "blosclz", "lz4", "lz4hc", "zlib", "snappy"]
vcf_to_zarr_blosc_shuffle_types = [Blosc.NOSHUFFLE, Blosc.SHUFFLE, Blosc.BITSHUFFLE, Blosc.AUTOSHUFFLE]


class VCFtoZarrConfigurationRepresentation:
    """ Utility class for object representation of VCF to Zarr conversion module configuration. """
    enabled = False  # Specifies whether the VCF to Zarr conversion module should be enabled or not
    fields = None
    alt_number = None  # Alt number to use when converting to Zarr format. If None, then this will need to be determined
    chunk_length = None  # Number of variants of chunks in which data are processed. If None, use default value
    chunk_width = None  # Number of samples to use when storing chunks in output. If None, use default value
    compressor = "Blosc"  # Specifies compressor type to use for Zarr conversion
    blosc_compression_algorithm = "zstd"
    blosc_compression_level = 1  # Level of compression to use for Zarr conversion
    blosc_shuffle_mode = Blosc.AUTOSHUFFLE

    def __init__(self, runtime_config=None):
        """
        Creates an object representation of VCF to Zarr Conversion module configuration data.
        :param runtime_config: runtime_config data to extract conversion configuration from
        :type runtime_config: ConfigurationRepresentation
        """
        if runtime_config is not None:
            # Check if [vcf_to_zarr] section exists in config
            if hasattr(runtime_config, "vcf_to_zarr"):
                # Extract relevant settings from config file
                if "enabled" in runtime_config.vcf_to_zarr:
                    self.enabled = config_str_to_bool(runtime_config.vcf_to_zarr["enabled"])
                if "alt_number" in runtime_config.vcf_to_zarr:
                    alt_number_str = runtime_config.vcf_to_zarr["alt_number"]

                    if str(alt_number_str).lower() == "auto":
                        self.alt_number = None
                    elif isint(alt_number_str):
                        self.alt_number = int(alt_number_str)
                    else:
                        raise TypeError("Invalid value provided for alt_number in configuration.\n"
                                        "Expected: \"auto\" or integer value")
                if "chunk_length" in runtime_config.vcf_to_zarr:
                    chunk_length_str = runtime_config.vcf_to_zarr["chunk_length"]
                    if chunk_length_str == "default":
                        self.chunk_length = None
                    elif isint(chunk_length_str):
                        self.chunk_length = int(chunk_length_str)
                    else:
                        raise TypeError("Invalid value provided for chunk_length in configuration.\n"
                                        "Expected: \"default\" or integer value")
                if "chunk_width" in runtime_config.vcf_to_zarr:
                    chunk_width_str = runtime_config.vcf_to_zarr["chunk_width"]
                    if chunk_width_str == "default":
                        self.chunk_width = None
                    elif isint(chunk_width_str):
                        self.chunk_width = int(chunk_width_str)
                    else:
                        raise TypeError("Invalid value provided for chunk_width in configuration.\n"
                                        "Expected: \"default\" or integer value")
                if "compressor" in runtime_config.vcf_to_zarr:
                    compressor_temp = runtime_config.vcf_to_zarr["compressor"]
                    # Ensure compressor type specified is valid
                    if compressor_temp in vcf_to_zarr_compressor_types:
                        self.compressor = compressor_temp
                if "blosc_compression_algorithm" in runtime_config.vcf_to_zarr:
                    blosc_compression_algorithm_temp = runtime_config.vcf_to_zarr["blosc_compression_algorithm"]
                    if blosc_compression_algorithm_temp in vcf_to_zarr_blosc_algorithm_types:
                        self.blosc_compression_algorithm = blosc_compression_algorithm_temp
                if "blosc_compression_level" in runtime_config.vcf_to_zarr:
                    blosc_compression_level_str = runtime_config.vcf_to_zarr["blosc_compression_level"]
                    if isint(blosc_compression_level_str):
                        compression_level_int = int(blosc_compression_level_str)
                        if (compression_level_int >= 0) and (compression_level_int <= 9):
                            self.blosc_compression_level = compression_level_int
                        else:
                            raise ValueError("Invalid value for blosc_compression_level in configuration.\n"
                                             "blosc_compression_level must be between 0 and 9.")
                    else:
                        raise TypeError("Invalid value for blosc_compression_level in configuration.\n"
                                        "blosc_compression_level could not be converted to integer.")
                if "blosc_shuffle_mode" in runtime_config.vcf_to_zarr:
                    blosc_shuffle_mode_str = runtime_config.vcf_to_zarr["blosc_shuffle_mode"]
                    if isint(blosc_shuffle_mode_str):
                        blosc_shuffle_mode_int = int(blosc_shuffle_mode_str)
                        if blosc_shuffle_mode_int in vcf_to_zarr_blosc_shuffle_types:
                            self.blosc_shuffle_mode = blosc_shuffle_mode_int
                        else:
                            raise ValueError("Invalid value for blosc_shuffle_mode in configuration.\n"
                                             "blosc_shuffle_mode must be a valid integer.")
                    else:
                        raise TypeError("Invalid value for blosc_shuffle_mode in configuration.\n"
                                        "blosc_shuffle_mode could not be converted to integer.")


benchmark_data_input_types = ["vcf", "zarr"]


class BenchmarkConfigurationRepresentation:
    """ Utility class for object representation of the benchmark module's configuration. """
    benchmark_number_runs = 5
    benchmark_data_input = "vcf"
    benchmark_dataset = ""
    benchmark_allele_count = False
    benchmark_PCA = False
    vcf_to_zarr_config = None

    def __init__(self, runtime_config=None):
        """
        Creates an object representation of the Benchmark module's configuration data.
        :param runtime_config: runtime_config data to extract benchmark configuration from
        :type runtime_config: ConfigurationRepresentation
        """
        if runtime_config is not None:
            if hasattr(runtime_config, "benchmark"):
                # Extract relevant settings from config file
                if "benchmark_number_runs" in runtime_config.benchmark:
                    try:
                        self.benchmark_number_runs = int(runtime_config.benchmark["benchmark_number_runs"])
                    except ValueError:
                        pass
                if "benchmark_data_input" in runtime_config.benchmark:
                    benchmark_data_input_temp = runtime_config.benchmark["benchmark_data_input"]
                    if benchmark_data_input_temp in benchmark_data_input_types:
                        self.benchmark_data_input = benchmark_data_input_temp
                if "benchmark_dataset" in runtime_config.benchmark:
                    self.benchmark_dataset = runtime_config.benchmark["benchmark_dataset"]
                if "benchmark_allele_count" in runtime_config.benchmark:
                    self.benchmark_allele_count = config_str_to_bool(runtime_config.benchmark["benchmark_allele_count"])
                if "benchmark_PCA" in runtime_config.benchmark:
                    self.benchmark_PCA = config_str_to_bool(runtime_config.benchmark["benchmark_PCA"])

            # Add the VCF to Zarr Conversion Configuration Data
            self.vcf_to_zarr_config = VCFtoZarrConfigurationRepresentation(runtime_config=runtime_config)


def read_configuration(location):
    """
    Args: location of the configuration file, existing configuration dictionary
    Returns: a dictionary of the form
    <dict>.<section>[<option>] and the corresponding values.
    """
    config = ConfigurationRepresentation(location)
    return config


def generate_default_config_file(output_location, overwrite=False):
    # Get Default Config File Data as Package Resource
    default_config_file_data = resource_string(__name__, 'config/benchmark.conf.default')

    if overwrite is None:
        overwrite = False

    if output_location is not None:
        # Check if a file currently exists at the location
        if os.path.exists(output_location) and not overwrite:
            print(
                "[Config] Could not generate configuration file: file exists at specified destination and overwrite mode disabled.")
            return

        # Write the default configuration file to specified location
        with open(output_location, 'wb') as output_file:
            output_file.write(default_config_file_data)

        # Check whether configuration file now exists and report status
        if os.path.exists(output_location):
            print("[Config] Configuration file has been generated successfully.")
        else:
            print("[Config] Configuration file was not generated.")
