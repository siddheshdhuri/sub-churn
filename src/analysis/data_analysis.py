# This class implements functionality to analyse data
import sys
import pandas as pd
import os
import codecs
import pandas as pd
import matplotlib.pyplot as plt
from fpdf import FPDF
from textwrap import wrap
import gc

class DataAnalysis:

    @staticmethod
    # get a summary of the dataframe, including each column's unique values, value counts, the percentage of missing data,
    # and the datatype of the column
    def get_data_frame_summmary(data_frame):
        unique_values = data_frame.apply(lambda x: [x.unique()])
        unique_counts = data_frame.apply(lambda x: len(x.unique()))
        percent_missing = data_frame.apply(lambda x: sum(pd.isnull(x))/len(x)*100)
        data_type = data_frame.dtypes 

        # convert to a dataframe
        return pd.DataFrame(dict(unique_values = unique_values, 
                                unique_counts = unique_counts,
                                data_type = data_type,
                                percent_missing = percent_missing,
                                )).reset_index().sort_values(by='percent_missing', ascending=False)

    @staticmethod
    def get_freq_table(data_frame, col_name):
        return data_frame[col_name].value_counts()

    # generate a summary file
    def generate_summary_file(summary_file, pickle_dir, freq_count_limit=50):
        sys.stdout = open(summary_file, "wb+")
        sys.stdout = codecs.getwriter("iso-8859-1")(sys.stdout, 'xmlcharrefreplace')
        for filename in os.listdir(pickle_dir):        
            print(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            print(f' - TABLE {filename} SUMMARY ')
            print(f'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
            
            df = pd.read_pickle(os.path.join(pickle_dir, filename))

            if df is None:
                print('DATAFRAME IS NONE - UNABLE TO READ DATAFRAME')
                print(f'======================================= - END - =============================================')
                continue

            df_summary = DataAnalysis.get_data_frame_summmary(df)

            print(df_summary)
            print(" ")

            #' freq table for counts less than 50
            df_freq = df_summary[df_summary.unique_counts < freq_count_limit]

            for colname in df_freq['index']:
                print(f'=============================================================================================')
                print(f' - COLUMN: {colname} FREQ TABLE')
                print(f'=============================================================================================')
                freq_table = DataAnalysis.get_freq_table(df, colname)                
                print(freq_table)
                print(" ")

            print(f'======================================= - END {filename} - =============================================')
            print(" ")
            print(" ")
            

        sys.stdout = sys.__stdout__

    @staticmethod
    def make_charts(config):
        # read all pickle files in as DataFrames
        for rootdir, dir, files in os.walk(config.CLEAN_PICKLES_FOR_GRAPHS_DIR):
            for file in files:
                df = pd.read_pickle(os.path.join(rootdir, file))
                print(file)
                #Create a dictionary of graphs that we don't wan't to include in the PDF
                exclude_dict = {f'{config.CHURN_ACTIVITIES_FILE}_for_graphs.pickle': ['Task/Event Record Type', 'Task'],
                                f'{config.CHURN_RISKS_FILE}_for_graphs.pickle': [],
                                f'{config.ACCOUNT_ASSIGNMENT_FILE}_for_graphs.pickle': [],
                                f'{config.CANCELLATIONS_FILE}_for_graphs.pickle': ['Calculated New/Renewal', 'Product Revenue Type', 'Sales Type', 'Status'],
                                f'{config.JOURNAL_CONTRACTS_FILE}_for_graphs.pickle': ['Division', 'Sales Type', 'Status', 'WIP Flag'],
                                f'{config.OTHER_CONTRACTS_FILE}_for_graphs.pickle': ['Division', 'Payment Term Type', 'Product Line Level 1', 'Product Revenue Type', 'RSO', 'Sales Type', 'Status', 'Subregion Grouping', 'WIP Flag'],
                                f'{config.ECH_FILE}_for_graphs.pickle': [],
                                f'{config.INTERACTIONS_FILE}_for_graphs.pickle': ['CUSTOMER_CLASSIFICATION_TYPE', 'NUMBER_OF_RESPONSES', 'STATUS'],
                                f'{config.NPS_FILE}_for_graphs.pickle': ['AT_RISK', 'WAVE'],
                                f'{config.PRODUCT_ASSIGNMENT_FILE}_for_graphs.pickle': ['PRODUCT_LEVEL_1'],
                                f'{config.USAGE_FILE}_for_graphs.pickle': ['REPORT_DT']}
                # looping through the columns of the DataFrame, create graphs of columns with between 2 and 30 unique values
                for col_index in range(0, df.shape[1]-1):
                    print(df.keys()[col_index]) #print column name
                    if df.keys()[col_index] in exclude_dict[file]:
                        continue
                    unique = df.iloc[:, col_index].value_counts().rename_axis("unique_values").reset_index(name="counts") #returns the unique values in the given column and their frequencies
                    print(unique.dtypes)
                    print(unique)
                    if (unique.shape[0] > 30 or unique.shape[0] < 2): #only generate graphs with between 2 and 30 unique values
                        print(f'Too many or too few unique values in field "{df.keys()[col_index]}"')
                    else:
                        #plot graphs
                        plt.figure()
                        plt.barh(unique.unique_values.astype(str), unique.counts) #print horizontal bar plots
                        plt.xticks(rotation=45)
                        plt.title(f'Table: {file[:-7]}\n%s' % "\n".join(wrap(f'Column: {df.keys()[col_index]}', width=60)))
                        plt.xlabel('Frequency')
                        plt.ylabel('Value', rotation='vertical')
                        plt.tight_layout() #make all titles and labels fit the plot
                        print(df.keys()[col_index])
                        #export graphs as figures
                        plt.savefig(os.path.join(config.PLOTS_DIR, f'{file[:-7]}_{df.keys()[col_index].replace("/", " or ")}.png'))
                        plt.close()
                del df
                gc.collect()

        #compile the PDF from the generated images
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font('Arial', 'B', 16)
        pdf.cell(40, 10, 'Plots of Unique Values and their Frequencies by Column by Dataset', 0)
        for rootdir, dir, images in os.walk(config.PLOTS_DIR):
            for image in images:
                pdf.add_page()
                pdf.image(os.path.join(config.PLOTS_DIR, image), w=190)
            #export the pdf
            pdf.output(os.path.join(rootdir, 'Data Analysis Charts.pdf'))

