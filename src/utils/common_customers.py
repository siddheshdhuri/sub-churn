import config
import pandas as pd
import numpy as np
import pickle
import os

def get_pickles():

    for rootdir, dir, files in os.walk(config.CLEAN_PICKLES_DIR):
        for file in files:
            df = pd.read_pickle(os.path.join(rootdir, file))

def common_customers(dataset_1, dataset_2, column_1, column_2):

    common_count = 0
    common_list = []
    for value in dataset_1.loc[:, column_1].unique():
        if value in dataset_2.loc[:, column_2].unique():
            common_list.append(value)
            common_count += 1
    print(f'For {dataset_2}, the common customers with {config.CLEAN_PICKLES_DIR}/{config.ECH_FILE}_clean are {common_list}. \n'
          f'There are {common_count} common values \n'
          f'This makes up {(common_count / dataset_2[column_2].nunique())*100}% of the total customers in this dataset.')


dict = {f'{config.CLEAN_PICKLES_DIR}/{config.ACCOUNT_ASSIGNMENT_FILE}_clean': 'ERCID', f'{config.CLEAN_PICKLES_DIR}/{config.USAGE_FILE}_clean': 'ECR_ID',
        f'{config.CLEAN_PICKLES_DIR}/{config.NPS_FILE}_clean': 'ECR_ID', f'{config.CLEAN_PICKLES_DIR}/{config.INTERACTIONS_FILE}_clean': 'ECR_ID',
        f'{config.CLEAN_PICKLES_DIR}/{config.ECH_FILE}_clean': 'ecrid', f'{config.CLEAN_PICKLES_DIR}/{config.OTHER_CONTRACTS_FILE}_clean': 'SIS Id',
        f'{config.CLEAN_PICKLES_DIR}/{config.JOURNAL_CONTRACTS_FILE}_clean': 'SIS Id', f'{config.CLEAN_PICKLES_DIR}/{config.CANCELLATIONS_FILE}_clean': 'SIS Id',
        f'{config.CLEAN_PICKLES_DIR}/{config.CHURN_RISKS_FILE}_clean': 'Account Name: ECR Id', f'{config.CLEAN_PICKLES_DIR}/{config.CHURN_ACTIVITIES_FILE}_clean': 'ECR Id'}



dataset_1 = pd.read_pickle(f'{config.CLEAN_PICKLES_DIR}/{config.ECH_FILE}_clean.pickle')
print(f'{config.ECH_FILE}_clean')
column_1 = dict[f'{config.CLEAN_PICKLES_DIR}/{config.ECH_FILE}_clean']
print(column_1)
for rootdir, dir, files in os.walk(config.CLEAN_PICKLES_DIR):
    for file in files:
        #dataset_2 = pd.read_pickle(os.path.join(rootdir, file))
        print(file)
        #column_2 = dict[f'{config.CLEAN_PICKLES_DIR}/{file.replace(".pickle","")}']
        print(column_2)
        #common_customers(dataset_1, dataset_2, column_1, column_2)

