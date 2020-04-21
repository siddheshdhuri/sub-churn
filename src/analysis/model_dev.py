import pandas as pd


journals_contracts = pd.read_pickle('data/pickle/DataCR_from_2015_journals.pickle')

print('Sales Type')
print(journals_contracts['Sales Type'].value_counts())
print('WIP Flag')
print(journals_contracts['WIP Flag'].value_counts())
is_cancelled = journals_contracts['Sales Type'] == 'Cancelled'
print(journals_contracts[is_cancelled]['WIP Flag'].value_counts())

# print('Payment Term Type')
# print(journals_contracts['Payment Term Type'].value_counts())
# print('Payment Term Description')
# print(journals_contracts['Payment Term Description'].value_counts())
# print('Payment Term')
# print(journals_contracts['Payment Term'].value_counts())
# print('Payment Term')
# print(journals_contracts['Payment Term'].value_counts())
# print('WIP Flag')
# print(journals_contracts['WIP Flag'].value_counts())
print('Sales Type')
print(journals_contracts['Sales Type'].value_counts())




# other_contracts = pd.read_pickle('data/pickle/DataCR_from_2015_other_products.pickle')
# print('Payment Term Type')
# print(other_contracts['Payment Term Type'].value_counts())
# print('Payment Term Description')
# print(other_contracts['Payment Term Description'].value_counts())
# print('Payment Term')
# print(other_contracts['Payment Term'].value_counts())
# print('Payment Term')
# print(other_contracts['Payment Term'].value_counts())
# print('WIP Flag')
# print(other_contracts['WIP Flag'].value_counts())
# print('Sales Type')
# print(other_contracts['Sales Type'].value_counts())

# cancelled_contracts = pd.read_pickle('data/pickle/DataCR_from_2015_Cancellations.pickle')
# print('WIP Flag')
# print(cancelled_contracts['WIP Flag'].value_counts())
# print('Sales Type')
# print(cancelled_contracts['Sales Type'].value_counts())
