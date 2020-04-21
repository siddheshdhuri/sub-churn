import yaml

cfg = None
    #' load config.yaml file in the root dir 
with open("config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

class Config:    

    START_DIR = cfg['start_dir']
    DEST_DIR = cfg['dest_dir']
    CLEAN_PICKLES_DIR = cfg['clean_pickles_dir']
    CLEAN_PICKLES_FOR_GRAPHS_DIR = cfg['clean_pickles_for_graphs_dir']
    USAGE_PREFIX = cfg['usage_prefix']
    INTERACTION_PREFIX = cfg['interaction_prefix']
    HIERARCHY_PREFIX = cfg['hierarchy_prefix']
    ACCOUNT_ASSIGNMENT_PREFIX = cfg['account_assignment_prefix']
    CONTRACTS_JOURNALS_PREFIX = cfg['contracts_journals_prefix']
    CONTRACTS_OTHER_PREFIX = cfg['contracts_other_prefix']
    USAGE_HEADERS_FILE = cfg['usage_headers_file']
    SCIENCE_DIRECT_HEADER_FILE = cfg['science_direct_header_file']
    INTERACTION_HEADERS_FILE = cfg['interaction_headers_file']
    SUMMARY_FILE = cfg['summary_file']
    PLOTS_DIR = cfg['plots_dir']
    DATA_DICTIONARY_FILE = cfg['data_dictionary_file']
    CHURN_ACTIVITIES_FILE = cfg['churn_activities_file']
    CHURN_RISKS_FILE = cfg['churn_risks_file']
    CHURN_PRODUCTS_FILE = cfg['churn_products_file']
    SIS_MAPPING_FILE = cfg['sis_mapping_file']
    ACCOUNT_ASSIGNMENT_FILE = cfg['account_assignment_file']
    CANCELLATIONS_FILE = cfg['cancellations_file']
    JOURNAL_CONTRACTS_FILE = cfg['journal_contracts_file']
    OTHER_CONTRACTS_FILE = cfg['other_contracts_file']
    ECH_FILE = cfg['ech_file']
    INTERACTIONS_FILE = cfg['interactions_file']
    NPS_FILE = cfg['nps_file']
    PRODUCT_ASSIGNMENT_FILE = cfg['product_assignment_file']
    USAGE_FILE = cfg['usage_file']
    SCIENCE_DIRECT_USAGE_FILE = cfg['science_direct_usage_file']
    HIERARCHY_FILE = cfg['hierarchy_file']
    IGNORE_FILE_PATTERN = cfg['ignore_file_pattern']
    HDF_STORE_LOCATION = cfg['hdf_store_location']
    CLEAN_HDF_STORE_LOCATION = cfg['clean_hdf_store_location']
    PATH_TO_HDF_DATASTORE = cfg['path_to_hdf_datastore']
    PATH_TO_CLEAN_HDF_DATASTORE = cfg['path_to_clean_hdf_datastore']
    CHURN_ACTIVITIES_HDF_FILE = cfg['churn_activities_hdf_file']
    CHURN_RISKS_HDF_FILE = cfg['churn_risks_hdf_file']
    ECH_HDF_FILE = cfg['ech_hdf_file']
    CHURN_PRODUCTS_HDF_FILE = cfg['churn_products_hdf_file']
