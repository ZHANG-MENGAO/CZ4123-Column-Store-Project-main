import csv
import os
import shutil
import heapq
import math

class ColumnStore:

    def __init__(
        self, 
        original_data_file:str, 
        column_store_folder:str, 
        results_folder:str, 
        zone_size:int, 
        chunk_size:int,
        mapper:dict,
        relevant_cols:list
    ) -> None:
        
        # deal with paths
        assert os.path.exists(original_data_file), "input data file not found"
        if not os.path.exists(column_store_folder):
            print("folder for storage not found, creating new folder: ")
            print(column_store_folder)
            os.makedirs(column_store_folder)
        if not os.path.exists(results_folder):
            print("folder for results not found, creating new folder: ")
            print(results_folder)
            os.makedirs(results_folder)
        temp_path = os.path.abspath("temp")
        if os.path.exists(temp_path):
            print("temp folder exists, removing the old temp files")
            # Iterate over all files and subdirectories in the folder
            for item in os.listdir(temp_path):
                item_path = os.path.join(temp_path, item)
                # Check if it's a file and remove it
                if os.path.isfile(item_path):
                    os.remove(item_path)
                # Check if it's a directory and remove it recursively
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            os.removedirs(temp_path)
        os.makedirs(temp_path)
        for col in relevant_cols:
            column_path = os.path.join(column_store_folder, f"{col}")
            if not os.path.exists(column_path):
                os.makedirs(column_path)
            print(f"making folder for column {col}")
        
        self.original_data_file = original_data_file
        self.column_store_folder = column_store_folder
        self.results_folder = results_folder
        self.zone_size = zone_size
        self.chunk_size = chunk_size
        self.mapper = mapper
        self.relevant_cols = relevant_cols

        self.temp_path = temp_path
        self.store_paths = {col:[] for col in self.relevant_cols}
        self.zone_maps = None

    def sort_and_store(self):
        # sort individual chunks
        temp_files = self.sort_chunks()
        self.temp_files = temp_files

        # Merge the temporary files and calculate stats for each attribute
        zone_maps = self.merge_chunks()
        self.zone_maps = zone_maps
        # print(zone_maps)
    
    def sort_chunks(self):
        temp_files = []
        chunk = []
        with open(self.original_data_file, 'r', newline='') as file:
            reader = csv.DictReader(file)
            
            for row_index, raw_row in enumerate(reader):
                # Extract attributes from the row and add row to chunk
                row = self.preprocess_row(raw_row, "dict")
                chunk.append(row)
                
                if (row_index + 1) % self.chunk_size == 0:
                    # Sort the chunk by the composite key before writing to temp file
                    sorted_chunk = sorted(chunk, key=self.composite_key_func)
                    temp_file_path = self.write_chunk_to_temp_files(sorted_chunk, len(temp_files))
                    temp_files.append(temp_file_path)
                    chunk = []  # Reset chunksfor next round

            # After all rows have been processed, handle any remaining rows in the last chunk
            if chunk:
                # Sort the chunk by the composite key before writing to temp file
                sorted_chunk = sorted(chunk, key=self.composite_key_func)
                temp_file_path = self.write_chunk_to_temp_files(sorted_chunk, len(temp_files))
                temp_files.append(temp_file_path)

        return temp_files

    def composite_key_func(self, row:dict):
        year = row['year']
        month = row['month']
        town = row['town']
        return (town, year, month)

    def write_chunk_to_temp_files(self, chunk, chunk_number):
        temp_file_path = f'temp_sorted_chunk_{chunk_number}.csv'
        temp_file_path = os.path.join(self.temp_path, temp_file_path)
        with open(temp_file_path, 'w', newline='') as temp_file:
            fieldnames = chunk[0].keys()
            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk)
        return temp_file_path

    def merge_chunks(self):
        pq = []
        file_handles = []
        zone_maps = []
        zone = {col:[] for col in self.relevant_cols}
        zone["indexes"] = []
        numbers = 0  # Counter for the number of records processed

        for temp_file_path in self.temp_files:
            try:
                file_handle = open(temp_file_path, 'r', newline='')
                reader = csv.DictReader(file_handle)
                file_handles.append((file_handle, reader))
                row = self.preprocess_row(next(reader), "list")
                heapq.heappush(pq, (row, file_handle, reader))
            except (FileNotFoundError, StopIteration) as e:
                print(f"Warning: file not found or empty: {temp_file_path}")
                continue

        while pq:
            row, file_handle, reader = heapq.heappop(pq)
            for col, val in zip(self.relevant_cols, row):
                zone[col].append(val)
            zone["indexes"].append(numbers)
            numbers += 1

            # If a new zone must be started or if it's the first zone, open a new file and reset stats
            if numbers % self.zone_size == 0:
                self.write_rows(zone)
                zone_stats = self.get_zone_stats(zone)
                zone_maps.append(zone_stats)
                zone = {col:[] for col in self.relevant_cols}
                zone["indexes"] = []
            
            try:
                next_row = self.preprocess_row(next(reader), "list")
                heapq.heappush(pq, (next_row, file_handle, reader))
            except StopIteration:
                file_handle.close()

        # Close the file handle if it is still open and update the stats for the last zone
        if len(zone['indexes']) != 0:
            if numbers > 0:
                zone_stats = self.get_zone_stats(zone)  # Update zone stats for the last, potentially partial, zone
                zone_maps.append(zone_stats)

        # Clean up file handles and temporary files
        for file_handle, _ in file_handles:
            file_handle.close()
        for temp_file_path in self.temp_files:
            os.remove(temp_file_path)

        return zone_maps

    def preprocess_row(self, row:dict, return_type:str):
        # this function processes row read from any csv file
        def process_dict(row, col, value):
            row[col] = value
        
        def process_list(row, col, value):
            row.append(value)
        
        if return_type=="dict":
            process_func = process_dict
            new_row = {}
        elif return_type=="list":
            process_func = process_list
            new_row = []
        else:
            raise NotImplementedError(f"return_type {return_type} is not implemented")
        
        try:
            cols = self.relevant_cols if len(self.relevant_cols) <= len(row.keys()) else row.keys()
            if len(row.keys()) == 2: # processing column stored rows
                value_mapping = {col:float(row[col]) if "." in row[col] else int(row[col]) for col in cols}
            else:
                value_mapping = {
                    "year": int(row["year"]) if "year" in row else int(row["month"].split("-")[0]),
                    "month": int(row["month"]) if "year" in row else int(row["month"].split("-")[1]),
                    "town": int(row['town']) if row['town'].replace("-", "").isdigit() \
                            else self.mapper['town2num'].get(row['town'], -1)
                }
            for col in cols:
                value = value_mapping.get(col, None) # uses None because float(row[col]) may cause bugs
                if value is None:
                    value = float(row[col])
                process_func(new_row, col, value)
                
        except:
            raise ValueError(f"Unknown column: {col}")
        
        return new_row

    def write_rows(self, rows:dict[list]):
        """
        This function write each column of rows into seperate files to implement column store
        """
        num_rows = len(rows["indexes"])
        num_zones = len(self.store_paths[self.relevant_cols[0]])
        for col, value_list in rows.items():
            if col=="indexes":
                continue
            assert len(value_list) == num_rows, f"number of rows are not consistent for column {col}"

            store_path = os.path.join(self.column_store_folder, f"{col}", f"{num_zones}.csv")
            with open(store_path, "w", newline='') as file:
                field_names = ["index", col]
                writer = csv.DictWriter(file, fieldnames=field_names)
                writer.writeheader()
                rows_col = [{"index": idx, col:val} for idx, val in zip(rows["indexes"], value_list)]
                writer.writerows(rows_col)
                self.store_paths[col].append(store_path)

    def get_zone_stats(self, zone:dict):
        zone_stat = {}
        # Calculate and store zone statistics
        for col, value_list in zone.items():
            if col=="indexes":
                continue
            zone_stat[col] = {}
            mini, maxi, mean, std_dev, n= self.calculate_statistics(value_list)
            zone_stat[col]["min"] = mini
            zone_stat[col]["max"] = maxi
            zone_stat[col]["avg"] = mean
            zone_stat[col]["std"] = std_dev

        zone_stat["record_count"] = len(zone["indexes"])
        zone_stat["index_min"] = min(zone["indexes"])
        zone_stat["index_max"] = max(zone["indexes"])
        return zone_stat

    def calculate_statistics(self, data:list):
        n = len(data)
        mean = sum(data) / n
        variance = sum((x - mean) ** 2 for x in data) / n
        std_dev = math.sqrt(variance) if n > 1 else 0
        return min(data), max(data), mean, std_dev, n
    
# if __name__=="__main__":