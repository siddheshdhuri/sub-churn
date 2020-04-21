# import packages
import pandas as pd
import os
import ctypes.test.test_repr
from pandas import HDFStore,DataFrame 
from utils.config import Config as config
import re

#create dictionaies for column types (str or int)
str_cols_dict = {
    config.CHURN_ACTIVITIES_FILE: ['Opportunity', 'Created By', 'Account ID', 'Company / Account', 'Contact', 'Lead', 'Priority', 'Activity Type', 'Task/Event Record Type', 'Task Subtype', 'Event Subtype', 'Subject', 'Call Result', 'Topics Discussed', 'Comments', 'Full Comments', 'Follow Up Subject', 'Follow Up Notes', 'Name of Value Prop', 'Activity ID', 'Assigned', 'Date', 'Product Name', 'Assigned Role', 'Assigned Role Display', 'Created Date', 'Start', 'End', 'ECR Id', 'Parent ECR-ID'],
    config.CHURN_RISKS_FILE: ['Opportunity ID', 'Opportunity Name', 'Sales Type', 'Stage', 'Level 1 Product', 'Level 2 Product', 'Expected Close Date', 'Subscription Start Date', 'Amount (converted) Currency', 'Agreement Number', 'Account Name: ECR Id', 'Account Name: Account Name', 'Risk ID', 'Risk Name', 'Risk Type', 'Severity', 'Status', 'Created Date', 'Comments', 'Competitor: Account Name'],
    config.CHURN_PRODUCTS_FILE: ['Product Level 1', 'Product Level 2', 'Product Level 3', 'Product Level 4'],
    config.SIS_MAPPING_FILE: ['ACCOUNT_NAME', 'SIS_ID', 'HQ_SIS_ID', 'OLD_SIS_ID', 'OLD_HQ_SIS_ID', 'CRM_ID', 'CRM_HQ_ID'],
    config.ACCOUNT_ASSIGNMENT_FILE: ['BUSINESS_DIVISION', 'COUNTRY', 'CUSTOMER_NAME', 'ECRID', 'LEVEL_12', 'LEVEL_13', 'LEVEL_14', 'LEVEL_15', 'ORGANIZATION_TYPE', 'PROVINCE', 'SIZE', 'STATE', 'TERRITORY', 'TERRITORY_OWNER', 'TERRITORY_TYPE', 'TIER'],
    config.JOURNAL_CONTRACTS_FILE: ['Agreement End Date', 'Agreement Number', 'Agreement Start Date', 'Business Division (Agreement SIS)', 'Business Indicator', 'Calculated New/Renewal', 'Country Name (Agreement SIS)', 'Division', 'HQ SIS Id (Agreement SIS)', 'Invoice Date', 'Invoice Num', 'Name  (Agreement SIS)', 'Parent Agreement Number', 'Payment Term', 'Payment Term Description', 'Payment Term Type', 'Product Line Level 1', 'Product Line Level 2', 'Product Line Level 3', 'Product Line Level 4', 'Product Revenue Type', 'RSO', 'Renewal Exp Complete Date', 'SIS Id  (Agreement SIS)', 'Saleable Product Name (Source)', 'Sales Division (Agreement SIS)', 'Sales Type', 'Status', 'Status Change Date', 'Subregion Grouping', 'Subscription End Date', 'Subscription Start Date', 'WIP Flag'],
    config.OTHER_CONTRACTS_FILE: ['Agreement End Date', 'Agreement Number', 'Agreement Start Date', 'Business Division (Agreement SIS)', 'Business Indicator', 'Calculated New/Renewal', 'Country Name (Agreement SIS)', 'Division', 'HQ SIS Id (Agreement SIS)', 'Invoice Date', 'Invoice Num', 'Name  (Agreement SIS)', 'Parent Agreement Number', 'Payment Term', 'Payment Term Description', 'Payment Term Type', 'Product Line Level 1', 'Product Line Level 2', 'Product Line Level 3', 'Product Line Level 4', 'Product Revenue Type', 'RSO', 'Renewal Exp Complete Date', 'SIS Id  (Agreement SIS)', 'Saleable Product Name (Source)', 'Sales Division (Agreement SIS)', 'Sales Type', 'Status', 'Status Change Date', 'Subregion Grouping', 'Subscription End Date', 'Subscription Start Date', 'WIP Flag'],
    config.CANCELLATIONS_FILE: ['Source System', 'SIS Id  (Agreement SIS)', 'Source System.1', 'HQ SIS Id (Agreement SIS)', 'Name  (Agreement SIS)', 'Business Division (Agreement SIS)', 'Sales Division (Agreement SIS)', 'Division', 'RSO', 'Subregion Grouping', 'Country Name (Agreement SIS)', 'WIP Flag', 'Status', 'Wip Type', 'Business Indicator', 'Sales Type', 'Calculated New/Renewal', 'Payment Term', 'Payment Term Description', 'Payment Term Type', 'Status Change Date', 'Renewal Exp Complete Date', 'Product Revenue Type', 'Product Line Level 1', 'Product Line Level 2', 'Product Line Level 3', 'Product Line Level 4', 'Saleable Product Name (Source)', 'Agreement Number', 'Agreement Start Date', 'Agreement End Date', 'Subscription Start Date', 'Subscription End Date', 'Parent Agreement Number', 'Currency(Entered)', 'Cancellation Reason'],
    config.ECH_FILE: ['ecrid', 'name', 'city', 'Country ISO', 'Region', 'post_code', 'Classification'],
    config.INTERACTIONS_FILE: ['CONTACT_COUNTRY', 'CONTACT_REASON_LVL1_DESC', 'CONTACT_REASON_LVL2_DESC', 'CONTACT_REASON_LVL3_DESC', 'CONTACT_TYPE', 'CREATED_TO_CLOSED_DAYS', 'CREATED_TO_INITIAL_RESPONSE_DAYS', 'CUSTOMER_CLASSIFICATION_PRODUCT', 'CUSTOMER_CLASSIFICATION_ROLE', 'CUSTOMER_CLASSIFICATION_TYPE', 'ECR_ID', 'INCIDENT_AUTO_SOLVED', 'INCIDENT_CLOSED_DATETIME', 'INCIDENT_CREATED_DATETIME', 'INCIDENT_ID', 'INCIDENT_REOPENED', 'INCIDENT_SYSTEM', 'NUMBER_OF_RESPONSES', 'OWNER_ID', 'OWNER_NAME', 'RESOLUTION_CODE_LVL1_DESC', 'RESOLUTION_CODE_LVL2_DESC', 'RESOLUTION_CODE_LVL3_DESC', 'SOURCE_LVL1_DESC', 'SOURCE_LVL2_DESC', 'STATUS'],
    config.NPS_FILE: ['ECR_ID', 'DATE_OF_INTERVIEW', 'ORG_NAME', 'COUNTRY', 'ORGANIZATION', 'PRODUCT_NAME_ROLLUP', 'PRODUCT_DETAIL', 'JOB_ROLE', 'JOB_ROLE_GROUPED', 'COMPETITOR_SAT', 'COMPETITOR_NAME', 'DEPARTMENT', 'INFLUENCE', 'CSAT', 'CSAT_COMMENT', 'NPS_COMMENT', 'AT_RISK', 'VALUE_FOR_MONEY_SCORE', 'SHARE_WITH_CUST_DETAILS'],
    config.PRODUCT_ASSIGNMENT_FILE: ['TERRITORYNAME', 'OWNERNAME', 'OWNERID', 'BU', 'ASSIGNTOTERRITORYNAME', 'ASSIGNTOTERRITORYOWNERNAME', 'ASSIGNTOTERRITORYOWNERID', 'PRODUCT_LEVEL_1', 'PRODUCT_LEVEL_2'],
    config.USAGE_FILE: ['ACT_CLICK_DEPTH', 'ACT_DWELL_TIME_VISIT_MIN', 'ECR_ID', 'LOY_DWELL_TIME_USER_MIN', 'POP_ACTIVE_USERS', 'POP_PAGE_VIEWS', 'PROD_NAME', 'REPORT_AGG'],
    config.HIERARCHY_FILE: ['CHILD_ECR', 'CHILD_NAME', 'CONSORTIUM', 'COUNTRY_CHILD', 'COUNTRY_PARENT', 'HIERARCHY_TYPE', 'PARENT_ECR', 'PARENT_NAME']                 
}

float_cols_dict = {
    config.JOURNAL_CONTRACTS_FILE: ['Bookigns - Committed Print(Rep)', 'Bookings - Final Net Price - Agent Discount Amount(Rep)'],
    config.OTHER_CONTRACTS_FILE: ['Bookigns - Committed Print(Rep)', 'Bookings - Final Net Price - Agent Discount Amount(Rep)'],
    config.CANCELLATIONS_FILE: ['Bookigns - Committed Print(Rep)', 'Bookings - Final Net Price - Agent Discount Amount(Rep)']
}

# creating a HDF5 file
store = HDFStore('data/hdf/datastore.h5')

directory = 'data/pickle'
for filename in os.listdir(directory): 
    filename_stem = filename.replace(".pickle","")
    hdf_name = re.sub(r"^[0-9]+_", "", filename_stem)
    # replace space with underscore
    hdf_name = re.sub(r" ", "_", hdf_name)
    
    # ignore directories
    if filename_stem in ['clean pickles','clean pickles for graphs', 'DataCRother_full'] or hdf_name in [w.replace('/', '') for w in store.keys()]:
        print(f'{hdf_name} Already in Datastore and will not be added')
        continue
    
    thisdf = pd.read_pickle(os.sep.join([directory, filename]))
    print(filename_stem)
    # set string cols to string datatype
    if filename_stem in str_cols_dict.keys():
        thisdf[str_cols_dict[filename_stem]] = thisdf[str_cols_dict[filename_stem]].astype(str)

    # set float cols to float datatype
    if filename_stem in float_cols_dict.keys():
        thisdf[float_cols_dict[filename_stem]] = thisdf[float_cols_dict[filename_stem]].astype(float)

     # save to datastore
    thisdf.to_hdf('data/hdf/datastore.h5', key=hdf_name)
    del thisdf

store.close() # ensure that you close the store
