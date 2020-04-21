import os
import pandas as pd
import fnmatch
from tqdm import tqdm
import re
from pandas import HDFStore
from utils.config import Config as config

class IngestData:  
    #function to read source system files into hdf files

    @staticmethod
    def read_data(source_dir, hdf_store_loc, ignore_file_pattern='', data_dict=None):       
        # create a dictionary of columns that should be of type 'str'
        str_cols_dict = {
            config.CHURN_ACTIVITIES_FILE: ['Opportunity', 'Created By', 'Account ID', 'Company / Account', 'Contact',
                                           'Lead', 'Priority', 'Activity Type', 'Task/Event Record Type',
                                           'Task Subtype', 'Event Subtype', 'Subject', 'Call Result',
                                           'Topics Discussed', 'Comments', 'Full Comments', 'Follow Up Subject',
                                           'Follow Up Notes', 'Name of Value Prop', 'Activity ID', 'Assigned', 'Date',
                                           'Product Name', 'Assigned Role', 'Assigned Role Display', 'Created Date',
                                           'Start', 'End', 'ECR Id', 'Parent ECR-ID'],
            config.CHURN_RISKS_FILE: ['Opportunity ID', 'Opportunity Name', 'Sales Type', 'Stage', 'Level 1 Product',
                                      'Level 2 Product', 'Expected Close Date', 'Subscription Start Date',
                                      'Amount (converted) Currency', 'Agreement Number', 'Account Name: ECR Id',
                                      'Account Name: Account Name', 'Risk ID', 'Risk Name', 'Risk Type', 'Severity',
                                      'Status', 'Created Date', 'Comments', 'Competitor: Account Name'],
            config.CHURN_PRODUCTS_FILE: ['Product Level 1', 'Product Level 2', 'Product Level 3', 'Product Level 4'],
            config.SIS_MAPPING_FILE: ['ACCOUNT_NAME', 'SIS_ID', 'HQ_SIS_ID', 'OLD_SIS_ID', 'OLD_HQ_SIS_ID', 'CRM_ID',
                                      'CRM_HQ_ID'],
            config.ACCOUNT_ASSIGNMENT_PREFIX: ['BUSINESS_DIVISION', 'COUNTRY', 'CUSTOMER_NAME', 'ECRID', 'LEVEL_12',
                                             'LEVEL_13', 'LEVEL_14', 'LEVEL_15', 'ORGANIZATION_TYPE', 'PROVINCE',
                                             'SIZE', 'STATE', 'TERRITORY', 'TERRITORY_OWNER', 'TERRITORY_TYPE', 'TIER'],
            config.CONTRACTS_JOURNALS_PREFIX: ['Agreement End Date', 'Agreement Number', 'Agreement Start Date',
                                            'Business Division (Agreement SIS)', 'Business Indicator',
                                            'Calculated New/Renewal', 'Country Name (Agreement SIS)', 'Division',
                                            'HQ SIS Id (Agreement SIS)', 'Invoice Date', 'Invoice Num',
                                            'Name  (Agreement SIS)', 'Parent Agreement Number', 'Payment Term',
                                            'Payment Term Description', 'Payment Term Type', 'Product Line Level 1',
                                            'Product Line Level 2', 'Product Line Level 3', 'Product Line Level 4',
                                            'Product Revenue Type', 'RSO', 'Renewal Exp Complete Date',
                                            'SIS Id  (Agreement SIS)', 'Saleable Product Name (Source)',
                                            'Sales Division (Agreement SIS)', 'Sales Type', 'Status',
                                            'Status Change Date', 'Subregion Grouping', 'Subscription End Date',
                                            'Subscription Start Date', 'WIP Flag'],
            config.CONTRACTS_OTHER_PREFIX: ['Agreement End Date', 'Agreement Number', 'Agreement Start Date',
                                          'Business Division (Agreement SIS)', 'Business Indicator',
                                          'Calculated New/Renewal', 'Country Name (Agreement SIS)', 'Division',
                                          'HQ SIS Id (Agreement SIS)', 'Invoice Date', 'Invoice Num',
                                          'Name  (Agreement SIS)', 'Parent Agreement Number', 'Payment Term',
                                          'Payment Term Description', 'Payment Term Type', 'Product Line Level 1',
                                          'Product Line Level 2', 'Product Line Level 3', 'Product Line Level 4',
                                          'Product Revenue Type', 'RSO', 'Renewal Exp Complete Date',
                                          'SIS Id  (Agreement SIS)', 'Saleable Product Name (Source)',
                                          'Sales Division (Agreement SIS)', 'Sales Type', 'Status',
                                          'Status Change Date', 'Subregion Grouping', 'Subscription End Date',
                                          'Subscription Start Date', 'WIP Flag'],
            config.CANCELLATIONS_FILE: ['Source System', 'SIS Id  (Agreement SIS)', 'Source System',
                                        'HQ SIS Id (Agreement SIS)', 'Name  (Agreement SIS)',
                                        'Business Division (Agreement SIS)', 'Sales Division (Agreement SIS)',
                                        'Division', 'RSO', 'Subregion Grouping', 'Country Name (Agreement SIS)',
                                        'WIP Flag', 'Status', 'Wip Type', 'Business Indicator', 'Sales Type',
                                        'Calculated New/Renewal', 'Payment Term', 'Payment Term Description',
                                        'Payment Term Type', 'Status Change Date', 'Renewal Exp Complete Date',
                                        'Product Revenue Type', 'Product Line Level 1', 'Product Line Level 2',
                                        'Product Line Level 3', 'Product Line Level 4',
                                        'Saleable Product Name (Source)', 'Agreement Number', 'Agreement Start Date',
                                        'Agreement End Date', 'Subscription Start Date', 'Subscription End Date',
                                        'Parent Agreement Number', 'Currency(Entered)', 'Cancellation Reason'],
            config.ECH_FILE: ['ecrid', 'name', 'city', 'Country ISO', 'Region', 'post_code', 'Classification'],
            config.INTERACTION_PREFIX: ['CONTACT_COUNTRY', 'CONTACT_REASON_LVL1_DESC', 'CONTACT_REASON_LVL2_DESC',
                                       'CONTACT_REASON_LVL3_DESC', 'CONTACT_TYPE', 'CREATED_TO_CLOSED_DAYS',
                                       'CREATED_TO_INITIAL_RESPONSE_DAYS', 'CUSTOMER_CLASSIFICATION_PRODUCT',
                                       'CUSTOMER_CLASSIFICATION_ROLE', 'CUSTOMER_CLASSIFICATION_TYPE', 'ECR_ID',
                                       'INCIDENT_AUTO_SOLVED', 'INCIDENT_CLOSED_DATETIME', 'INCIDENT_CREATED_DATETIME',
                                       'INCIDENT_ID', 'INCIDENT_REOPENED', 'INCIDENT_SYSTEM', 'NUMBER_OF_RESPONSES',
                                       'OWNER_ID', 'OWNER_NAME', 'RESOLUTION_CODE_LVL1_DESC',
                                       'RESOLUTION_CODE_LVL2_DESC', 'RESOLUTION_CODE_LVL3_DESC', 'SOURCE_LVL1_DESC',
                                       'SOURCE_LVL2_DESC', 'STATUS'],
            config.NPS_FILE: ['ECR_ID', 'DATE_OF_INTERVIEW', 'ORG_NAME', 'COUNTRY', 'ORGANIZATION',
                              'PRODUCT_NAME_ROLLUP', 'PRODUCT_DETAIL', 'JOB_ROLE', 'JOB_ROLE_GROUPED', 'COMPETITOR_SAT',
                              'COMPETITOR_NAME', 'DEPARTMENT', 'INFLUENCE', 'CSAT', 'CSAT_COMMENT', 'NPS_COMMENT',
                              'AT_RISK', 'VALUE_FOR_MONEY_SCORE', 'SHARE_WITH_CUST_DETAILS'],
            config.PRODUCT_ASSIGNMENT_FILE: ['TERRITORYNAME', 'OWNERNAME', 'OWNERID', 'BU', 'ASSIGNTOTERRITORYNAME',
                                             'ASSIGNTOTERRITORYOWNERNAME', 'ASSIGNTOTERRITORYOWNERID',
                                             'PRODUCT_LEVEL_1', 'PRODUCT_LEVEL_2'],
            config.USAGE_PREFIX: ['ACT_CLICK_DEPTH', 'ACT_DWELL_TIME_VISIT_MIN', 'ECR_ID', 'LOY_DWELL_TIME_USER_MIN',
                                'POP_ACTIVE_USERS', 'POP_PAGE_VIEWS', 'PROD_NAME', 'REPORT_AGG'],
            config.HIERARCHY_PREFIX: ['CHILD_ECR', 'CHILD_NAME', 'CONSORTIUM', 'COUNTRY_CHILD', 'COUNTRY_PARENT',
                                    'HIERARCHY_TYPE', 'PARENT_ECR', 'PARENT_NAME']
        }

        # create a dictionary of columns that should be of type 'float'
        float_cols_dict = {
            config.JOURNAL_CONTRACTS_FILE: ['Bookigns - Committed Print(Rep)',
                                            'Bookings - Final Net Price - Agent Discount Amount(Rep)'],
            config.OTHER_CONTRACTS_FILE: ['Bookigns - Committed Print(Rep)',
                                          'Bookings - Final Net Price - Agent Discount Amount(Rep)'],
            config.CANCELLATIONS_FILE: ['Bookigns - Committed Print(Rep)',
                                        'Bookings - Final Net Price - Agent Discount Amount(Rep)']
        }

        print(f'Reading files from {source_dir}')
        print(f'New Data store will be create at {hdf_store_loc}')
        # open the datastore and, if it exists, remove current content
        store = HDFStore(hdf_store_loc)
        print(f'Clean HDF Store if it alredy exists. Store currently contains following data {store.keys()}')
        for key in store.keys():
            store.remove(key)
        print(f'After cleaning store currently contains {store.keys()}')

        # go through directory of files that you want to ingest
        for (dirpath, dirnames, filenames) in tqdm(os.walk(os.path.normpath(source_dir))):
            hdf_name = None
            for filename in filenames:                
                # Ignore files that are marked as old
                ignore_this_file = False
                for p in ignore_file_pattern:
                    if fnmatch.fnmatch(filename, p): ignore_this_file = True

                if ignore_this_file: continue

                filedata = None  

                # ingest .csvs
                if filename.endswith('.csv'):                             
                    filedata = pd.read_csv(os.altsep.join([dirpath, filename]), encoding = "ISO-8859-1", low_memory=False)                    
                    hdf_name = str(filename.replace('.csv','')) + '.pickle'
                    # sometimes csv file colum headers have odd special characters we replace them
                    filedata.columns = [w.replace('ï»¿','') for w in filedata.columns] 

                elif filename.endswith('.csv.gz'):            

                    headers = pd.read_excel(os.sep.join([dirpath, data_dict]))['Column Name'] 
                    filedata = pd.read_csv(os.sep.join([dirpath, filename]),compression='gzip', header=None, names=headers)
                    if 'usage'.upper() in dirpath.upper():                        
                        filename = filename.replace('data_', 'usage_')
                    elif 'interaction'.upper() in dirpath.upper():
                        filename = filename.replace('data_', 'interaction_')
                    elif 'hierarchy'.upper() in dirpath.upper():                                       
                        filename = filename.replace('data_', 'hierarchy_')
                   
                    hdf_name = str(filename.replace('.csv.gz', '')) + '.pickle'
                elif filename.endswith('.xlsx'): 
                    filedata = pd.read_excel(os.sep.join([dirpath, filename]))
                    
                    hdf_name = str(filename.replace('.xlsx', '')) + '.pickle'
                else:
                    print(f'File {filename} is not read as it is not csv /gz/ xlsx format ')
                    continue

                # data stored in an hdf datastore cannot have a name which starts with numbers, and it shouldn't contain spaces
                # replace these
                filename_stem = hdf_name.replace(".pickle", "")
                hdf_name = re.sub(r"^[0-9]+_", "", filename_stem)
                # replace space with underscore
                hdf_name = re.sub(r" ", "_", hdf_name)

                dict_lookup = filename_stem.split('_')[0]
                if "journals" in filename_stem:
                    dict_lookup = dict_lookup+'*journals'
                elif "other" in filename_stem:
                    dict_lookup = dict_lookup+'*other'
                elif config.ACCOUNT_ASSIGNMENT_PREFIX in filename_stem:
                    dict_lookup =  config.ACCOUNT_ASSIGNMENT_PREFIX
                elif config.USAGE_PREFIX in filename_stem:
                    dict_lookup =  config.USAGE_PREFIX
                elif config.INTERACTION_PREFIX in filename_stem:
                    dict_lookup = config.INTERACTION_PREFIX
                elif config.HIERARCHY_PREFIX in filename_stem:
                    dict_lookup = config.HIERARCHY_PREFIX
                else:
                    dict_lookup = filename_stem

                print(f'FILESTEM : {filename_stem.split("_")[0]} to be matched for {filename_stem}')
                
                try:
                    # set string cols to string datatype
                    if dict_lookup in str_cols_dict.keys():
                        filedata[str_cols_dict[dict_lookup]] = filedata[str_cols_dict[dict_lookup]].astype(str)

                    # set float cols to float datatype
                    if dict_lookup in float_cols_dict.keys():
                        filedata[float_cols_dict[dict_lookup]] = filedata[float_cols_dict[dict_lookup]].astype(float)
                except:
                    print('Exception while typecasting columns, please check columns are same as in the declatred dictionary above')

                if hdf_name is not None:                    
                    # save hdf file
                    #filedata.to_hdf(store, key=hdf_name)
                    store.put(key=hdf_name, value=filedata)
                del filedata

        # store should always be closed after use
        store.close()


    @staticmethod
    def aggregate(hdf_store_loc, file_pattern, headerfile=None, remove_part_files=False):
        df = None           

        store = HDFStore(hdf_store_loc)
        store_keys = [w.replace('/', '') for w in store.keys()]           

        print(f'Aggregating part files in {hdf_store_loc} for {file_pattern} into single file')

        for key in store_keys:
            if re.match(file_pattern.replace('*','.+'), key):
                print(f'********************* Key : {key} MAtches pattern : {file_pattern.replace("*",".+")}')
                #thisdf = pd.read_hdf(store_loc, key)
                thisdf = store.select(key)

                if df is None:
                    df = thisdf
                else:
                    #' for gz file that not have headers assign headers.
                    try:
                        df = df.append(thisdf, ignore_index=True, sort=True)                        
                    except Exception as e:
                        print('Error while joining data {e}')

                if remove_part_files:
                    store.remove(key)

        
        try:       
            #df.to_hdf(store_loc, key=file_pattern.replace('*',''))
            store.put(key= file_pattern.replace('*',''), value=df)
        except Exception as e:
            print(f'Exception while combining flile for {file_pattern} exception {e}')    

        store.close()
        