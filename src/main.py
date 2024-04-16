import os
from project_config import (
    COLUMN_STORE_FOLDER,
    ORIGINAL_DATA_FILE,
    RESULTS_FOLDER,
    TEMP_FILE_SIZE,
    ZONE_SIZE,
    MAPPER,
    RELEVANT_COLS,
    QUERY_TYPES
)
from typing import List, Dict, Tuple
from Processor import Processor
from columnStore import ColumnStore
    
def main() -> None:
    """Main interface with user"""
    print(f'Data file used: {ORIGINAL_DATA_FILE}')
    print(f'File Size is {os.stat(ORIGINAL_DATA_FILE).st_size / (1024 * 1024)} MB')

    line_count = sum(1 for _ in open(ORIGINAL_DATA_FILE, 'r'))
    print(f'Number of Lines in the file is {line_count}')

    # initialize parameters for column store
    storage_manager = ColumnStore(original_data_file=ORIGINAL_DATA_FILE,
                                  column_store_folder=COLUMN_STORE_FOLDER,
                                  results_folder=RESULTS_FOLDER,
                                  zone_size=ZONE_SIZE,
                                  chunk_size=TEMP_FILE_SIZE,
                                  mapper=MAPPER,
                                  relevant_cols=RELEVANT_COLS
                                  )
    # do the sorting and column store
    storage_manager.sort_and_store()

    while True:
        print()
        text = 'Enter your matriculation number for processing, c to cancel: '
        matric_num = input(text).strip()
        if matric_num == 'c':
            print('Have a good day, bye bye...')
            break
        try:
            if len(matric_num) != 9:
                print('Invalid input, matriculation number is of length 9...')
                continue
        except ValueError:
            print('Invalid input, please try again...')
            continue
        
        text = "Enter the statistics to retrieve, c to cancel, h to see available statistics: "
        query = input(text).strip()
        if matric_num == 'c':
            print('Have a good day, bye bye...')
            break
        if query == 'h':
            print(QUERY_TYPES, sep="\n")
            continue

        try:
            if query not in QUERY_TYPES:
                print('Invalid query...')
                continue
        except ValueError:
            print('Invalid input, please try again...')
            continue

        processer = Processor(matric_num=matric_num, query=query, storage_manager=storage_manager)
        processer.process_data()


if __name__ == '__main__':
    main()
