# import packages
from utils.config import Config as config
from ingestion.ingest_data import IngestData as ingestor
from analysis.data_analysis import DataAnalysis as analyser
import sys
import time
from tqdm import tqdm

def main():

    # create a dictionary that specifies the headers of each dataset, if applicable
    aggregation_dict = {config.USAGE_PREFIX: None,
                        config.INTERACTION_PREFIX: None,
                        config.HIERARCHY_PREFIX: None,
                        config.ACCOUNT_ASSIGNMENT_PREFIX: None,
                        config.CONTRACTS_JOURNALS_PREFIX: None,
                        config.CONTRACTS_OTHER_PREFIX: None,
                        config.SCIENCE_DIRECT_USAGE_FILE: None}

    
    # Ingest data from the source systems in to Python native pickle files
    # and aggregate split files
    if "ingest" in sys.argv:
        print("Data ingestion started")
        start_time = time.time()
        ingestor.read_data(config.START_DIR, config.PATH_TO_HDF_DATASTORE, 
                           config.IGNORE_FILE_PATTERN, config.DATA_DICTIONARY_FILE)

        # Aggregate required files into one
        for files_to_aggregate in aggregation_dict:
            ingestor.aggregate(config.PATH_TO_HDF_DATASTORE, files_to_aggregate,
            headerfile=aggregation_dict[files_to_aggregate], remove_part_files=True)

        print(f'Data ingestion completed in {round((time.time() - start_time)/60, 2)} mins')

    # Generate a summary of the entire data set and write to the specified summary text file
    if "summarise" in sys.argv:
        print("Data summarization started")
        start_time = time.time()
        analyser.generate_summary_file(config.SUMMARY_FILE, config.DEST_DIR, freq_count_limit = 30)
        print(f'summary.txt file created in {round((time.time() - start_time)/60, 2)} mins')

    # Generate a summary of the entire data set and write to the specified summary text file
    if "charts" in sys.argv:
        print("Data charts started")
        start_time = time.time()
        analyser.make_charts(config)
        print(f'Charts for the data created in {round((time.time() - start_time)/60, 2)} mins')


if __name__ == '__main__':
    main()
