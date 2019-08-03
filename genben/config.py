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

    def __getitem__(self, item):
        return self.__dict__[item]


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


class DaskSchedulerConfigurationRepresentation:
    """ Utility class for object representation of the Dask scheduler module configuration. """
    enabled = False  # Specifies whether connection to a Dask scheduler should be performed or not
    scheduler_address = '127.0.0.1'
    scheduler_port = 8786

    def __init__(self, runtime_config=None):
        """
        Creates an object representation of Dask scheduler module configuration data.
        :param runtime_config: runtime_config data to extract Dask scheduler configuration from
        :type runtime_config: ConfigurationRepresentation
        """
        if runtime_config is not None:
            # Check if [vcf_to_zarr] section exists in config
            if hasattr(runtime_config, "dask"):
                # Extract relevant settings from config file
                config_dask = runtime_config['dask']
                if "enabled" in config_dask:
                    self.enabled = config_str_to_bool(config_dask["enabled"])
                if 'scheduler_address' in config_dask:
                    self.scheduler_address = config_dask['scheduler_address']
                if "scheduler_port" in config_dask:
                    scheduler_port_str = config_dask["scheduler_port"]
                    if isint(scheduler_port_str) and int(scheduler_port_str) > 0:
                        self.scheduler_port = int(scheduler_port_str)
                    else:
                        raise ValueError("Invalid value provided for scheduler port in configuration.\n"
                                         "Expected: positive integer value")


benchmark_data_input_types = ["vcf", "zarr"]

PCA_DATA_SCALER_STANDARD = 0
PCA_DATA_SCALER_PATTERSON = 1
PCA_DATA_SCALER_NONE = 2
benchmark_pca_data_scaler_types = {PCA_DATA_SCALER_STANDARD: 'standard',
                                   PCA_DATA_SCALER_PATTERSON: 'patterson',
                                   PCA_DATA_SCALER_NONE: None}

GENOTYPE_ARRAY_NORMAL = 0
GENOTYPE_ARRAY_DASK = 1
GENOTYPE_ARRAY_CHUNKED = 2
genotype_array_types = {GENOTYPE_ARRAY_NORMAL,
                        GENOTYPE_ARRAY_DASK,
                        GENOTYPE_ARRAY_CHUNKED}


class BenchmarkConfigurationRepresentation:
    """ Utility class for object representation of the benchmark module's configuration. """
    benchmark_number_runs = 5
    benchmark_data_input = "vcf"
    benchmark_dataset = ""
    benchmark_num_variants = -1
    benchmark_num_samples = -1
    benchmark_aggregations = False
    benchmark_pca = False
    genotype_array_type = GENOTYPE_ARRAY_DASK
    dask_genotype_array_chunk_variants = -1
    dask_genotype_array_chunk_samples = -1
    vcf_to_zarr_config = None
    results_output_config = None

    # PCA-specific settings
    pca_number_components = 10
    pca_data_scaler = benchmark_pca_data_scaler_types[PCA_DATA_SCALER_PATTERSON]
    pca_subset_size = 100000
    pca_ld_enabled = False
    pca_ld_pruning_number_iterations = 2
    pca_ld_pruning_size = 100
    pca_ld_pruning_step = 20
    pca_ld_pruning_threshold = 0.01

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
                if "benchmark_num_variants" in runtime_config.benchmark:
                    benchmark_num_variants_str = runtime_config.benchmark["benchmark_num_variants"]
                    if isint(benchmark_num_variants_str) and (
                            int(benchmark_num_variants_str) == -1 or int(benchmark_num_variants_str) >= 1):
                        self.benchmark_num_variants = int(benchmark_num_variants_str)
                    else:
                        raise ValueError("Invalid value for benchmark_num_variants in configuration.\n"
                                         "benchmark_num_variants must be a valid integer greater than 1.\n"
                                         "Alternatively, a value of -1 can be specified to include all variants.")
                if "benchmark_num_samples" in runtime_config.benchmark:
                    benchmark_num_samples_str = runtime_config.benchmark["benchmark_num_samples"]
                    if isint(benchmark_num_samples_str) and (
                            int(benchmark_num_samples_str) == -1 or int(benchmark_num_samples_str) >= 1):
                        self.benchmark_num_samples = int(benchmark_num_samples_str)
                    else:
                        raise ValueError("Invalid value for benchmark_num_samples in configuration.\n"
                                         "benchmark_num_samples must be a valid integer greater than 1.\n"
                                         "Alternatively, a value of -1 can be specified to include all samples.")
                if "benchmark_aggregations" in runtime_config.benchmark:
                    self.benchmark_aggregations = config_str_to_bool(runtime_config.benchmark["benchmark_aggregations"])
                if "benchmark_pca" in runtime_config.benchmark:
                    self.benchmark_pca = config_str_to_bool(runtime_config.benchmark["benchmark_pca"])
                if "genotype_array_type" in runtime_config.benchmark:
                    genotype_array_type_str = runtime_config.benchmark["genotype_array_type"]
                    if isint(genotype_array_type_str) and (
                            int(genotype_array_type_str) in genotype_array_types):
                        self.genotype_array_type = int(genotype_array_type_str)
                    else:
                        raise ValueError("Invalid value for genotype_array_type in configuration.\n"
                                         "genotype_array_type must be a valid integer between 0 and 2")
                if "dask_genotype_array_chunk_variants" in runtime_config.benchmark:
                    dask_genotype_array_chunk_variants_str = runtime_config.benchmark["dask_genotype_array_chunk_variants"]
                    if isint(dask_genotype_array_chunk_variants_str):
                        if int(dask_genotype_array_chunk_variants_str) == -1 or int(
                                dask_genotype_array_chunk_variants_str) > 0:
                            self.dask_genotype_array_chunk_variants = int(dask_genotype_array_chunk_variants_str)
                        else:
                            raise ValueError("Invalid value for dask_genotype_array_chunk_variants in configuration.\n"
                                             "dask_genotype_array_chunk_variants must be a valid integer equal to\n"
                                             "-1 or an integer greater than 0.")
                    else:
                        raise TypeError("Invalid type for dask_genotype_array_chunk_variants in configuration.\n"
                                        "dask_genotype_array_chunk_variants must be a valid integer equal to\n"
                                        "-1 or an integer greater than 0.")
                if "dask_genotype_array_chunk_samples" in runtime_config.benchmark:
                    dask_genotype_array_chunk_samples_str = runtime_config.benchmark["dask_genotype_array_chunk_samples"]
                    if isint(dask_genotype_array_chunk_samples_str):
                        if int(dask_genotype_array_chunk_samples_str) == -1 or int(
                                dask_genotype_array_chunk_samples_str) > 0:
                            self.dask_genotype_array_chunk_samples = int(dask_genotype_array_chunk_samples_str)
                        else:
                            raise ValueError("Invalid value for dask_genotype_array_chunk_samples in configuration.\n"
                                             "dask_genotype_array_chunk_samples must be a valid integer equal to\n"
                                             "-1 or an integer greater than 0.")
                    else:
                        raise TypeError("Invalid type for dask_genotype_array_chunk_samples in configuration.\n"
                                        "dask_genotype_array_chunk_samples must be a valid integer equal to\n"
                                        "-1 or an integer greater than 0.")
                if "pca_number_components" in runtime_config.benchmark:
                    pca_number_components_str = runtime_config.benchmark["pca_number_components"]
                    if isint(pca_number_components_str) and (int(pca_number_components_str) > 0):
                        self.pca_number_components = int(pca_number_components_str)
                    else:
                        raise ValueError("Invalid value for pca_number_components in configuration.\n"
                                         "pca_number_components must be a valid integer greater than 0.")
                if "pca_data_scaler" in runtime_config.benchmark:
                    pca_data_scaler_str = runtime_config.benchmark["pca_data_scaler"]
                    if isint(pca_data_scaler_str) and (int(pca_data_scaler_str) in benchmark_pca_data_scaler_types):
                        self.pca_data_scaler = benchmark_pca_data_scaler_types[int(pca_data_scaler_str)]
                    else:
                        raise ValueError("Invalid value for pca_data_scaler in configuration.\n"
                                         "pca_data_scaler must be a valid integer between 0 and 2")
                if "pca_subset_size" in runtime_config.benchmark:
                    pca_subset_size_str = runtime_config.benchmark["pca_subset_size"]
                    if isint(pca_subset_size_str) and (int(pca_subset_size_str) > 0):
                        self.pca_subset_size = int(pca_subset_size_str)
                    elif isint(pca_subset_size_str) and (int(pca_subset_size_str) == -1):
                        self.pca_subset_size = int(pca_subset_size_str)
                    else:
                        raise ValueError("Invalid value for pca_subset_size in configuration.\n"
                                         "pca_subset_size must be a valid integer greater than 0.\n"
                                         "Additionally, a value of -1 can be used to include all samples.")
                if "pca_ld_enabled" in runtime_config.benchmark:
                    self.pca_ld_enabled = config_str_to_bool(runtime_config.benchmark["pca_ld_enabled"])
                if "pca_ld_pruning_number_iterations" in runtime_config.benchmark:
                    pca_ld_pruning_number_iterations_str = runtime_config.benchmark["pca_ld_pruning_number_iterations"]
                    if isint(pca_ld_pruning_number_iterations_str) and (int(pca_ld_pruning_number_iterations_str) > 0):
                        self.pca_ld_pruning_number_iterations = int(pca_ld_pruning_number_iterations_str)
                    else:
                        raise ValueError("Invalid value for pca_ld_pruning_number_iterations in configuration.\n"
                                         "pca_ld_pruning_number_iterations must be a valid integer greater than 0.")
                if "pca_ld_pruning_size" in runtime_config.benchmark:
                    pca_ld_pruning_size_str = runtime_config.benchmark["pca_ld_pruning_size"]
                    if isint(pca_ld_pruning_size_str) and (int(pca_ld_pruning_size_str) > 0):
                        self.pca_ld_pruning_size = int(pca_ld_pruning_size_str)
                    else:
                        raise ValueError("Invalid value for pca_ld_pruning_size in configuration.\n"
                                         "pca_ld_pruning_size must be a valid integer greater than 0.")
                if "pca_ld_pruning_step" in runtime_config.benchmark:
                    pca_ld_pruning_step_str = runtime_config.benchmark["pca_ld_pruning_step"]
                    if isint(pca_ld_pruning_step_str) and (int(pca_ld_pruning_step_str) > 0):
                        self.pca_ld_pruning_step = int(pca_ld_pruning_step_str)
                    else:
                        raise ValueError("Invalid value for pca_ld_pruning_step in configuration.\n"
                                         "pca_ld_pruning_step must be a valid integer greater than 0.")
                if "pca_ld_pruning_threshold" in runtime_config.benchmark:
                    pca_ld_pruning_threshold_str = runtime_config.benchmark["pca_ld_pruning_threshold"]
                    if isfloat(pca_ld_pruning_threshold_str) and (float(pca_ld_pruning_threshold_str) > 0):
                        self.pca_ld_pruning_threshold = float(pca_ld_pruning_threshold_str)
                    else:
                        raise ValueError("Invalid value for pca_ld_pruning_threshold in configuration.\n"
                                         "pca_ld_pruning_threshold must be a valid float greater than 0.")

            # Add the VCF to Zarr Conversion Configuration Data
            self.vcf_to_zarr_config = VCFtoZarrConfigurationRepresentation(runtime_config=runtime_config)

            # Add the Output Results Configuration Data
            self.results_output_config = OutputConfigurationRepresentation(runtime_config=runtime_config)


class OutputConfigurationRepresentation:
    """ Utility class for object representation of the benchmark results output module's configuration. """
    output_csv_enabled = True
    output_csv_delimiter = '|'

    output_influxdb_enabled = False
    output_influxdb_host = 'localhost'
    output_influxdb_port = 8086
    output_influxdb_username = 'root'
    output_influxdb_password = 'root'
    output_influxdb_database_name = 'benchmark'
    output_influxdb_benchmark_group = ''
    output_influxdb_device_name = ''

    def __init__(self, runtime_config=None):
        """
        Creates an object representation of the results output module's configuration data.
        :param runtime_config: runtime_config data to extract output configuration from
        :type runtime_config: ConfigurationRepresentation
        """
        if runtime_config is not None:
            # Check if settings exist for [output.csv] module
            if hasattr(runtime_config, 'output.csv'):
                # Extract relevant settings from config file
                config_output_csv = runtime_config['output.csv']
                if 'enabled' in config_output_csv:
                    self.output_csv_enabled = config_str_to_bool(config_output_csv['enabled'])
                if 'delimiter' in config_output_csv:
                    self.output_csv_delimiter = config_output_csv['delimiter']

            # Check if settings exist for [output.csv] module
            if hasattr(runtime_config, 'output.influxdb'):
                # Extract relevant settings from config file
                config_output_influxdb = runtime_config['output.influxdb']
                if 'enabled' in config_output_influxdb:
                    self.output_influxdb_enabled = config_str_to_bool(config_output_influxdb['enabled'])
                if 'host' in config_output_influxdb:
                    self.output_influxdb_host = config_output_influxdb['host']
                if 'port' in config_output_influxdb:
                    config_output_influxdb_port_str = config_output_influxdb['port']
                    if isint(config_output_influxdb_port_str):
                        self.output_influxdb_port = int(config_output_influxdb_port_str)
                    else:
                        raise ValueError("Invalid value for port in [output.influxdb] configuration.\n"
                                         "port must be a valid integer.")
                if 'username' in config_output_influxdb:
                    self.output_influxdb_username = config_output_influxdb['username']
                if 'password' in config_output_influxdb:
                    self.output_influxdb_password = config_output_influxdb['password']
                if 'database_name' in config_output_influxdb:
                    self.output_influxdb_database_name = config_output_influxdb['database_name']
                if 'benchmark_group' in config_output_influxdb:
                    self.output_influxdb_benchmark_group = config_output_influxdb['benchmark_group']
                if 'device_name' in config_output_influxdb:
                    self.output_influxdb_device_name = config_output_influxdb['device_name']


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
