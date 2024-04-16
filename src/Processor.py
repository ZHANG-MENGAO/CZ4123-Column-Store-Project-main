from project_config import (
    RESULTS_FOLDER,
    KEY_MAPPING
)
from typing import List, Dict, Tuple, Set, Union
import os
import csv
import math
from columnStore import ColumnStore


class Processor:
    def __init__(
        self,
        matric_num: str,
        query:str,
        storage_manager: ColumnStore
    ) -> None:
        self.storage_manager = storage_manager
        self.matric_num = matric_num
        self.query = query.lower()
        self.year, self.start_month, self.end_month, self.town = self.get_month_year_town()

    def get_month_year_town(self):
        """Convert matric digits to month, year and town"""
        year = int(self.matric_num[-2])
        commence_month = int(self.matric_num[-3])
        town = int(self.matric_num[-4])
        if commence_month >=10 or year >= 10 or town >= 10:
            raise ValueError('Invalid values for month, year or town')
        
        if commence_month == 0:
            commence_month = 10
        
        if year<4:
            year = 2020 + year
        else:
            year = 2010 + year

        return year, commence_month, commence_month+2, town
    
    def process_data(self):
        """Process the data"""
        print(f"Processing data from year {self.year}, month {self.start_month} to {self.end_month} for town {self.town}...")
        self.zone_maps = self.storage_manager.zone_maps
        zone_indexes = self.get_relevant_zones(self.zone_maps)
        print("revelant zones")
        print(zone_indexes)

        stats = self.calculate_stats(zone_indexes)

        if stats is None:
            print("No results found")
        else:
            self.write_results(stats)

    def get_relevant_zones(self, zone_maps):
        relevant_zones = []
        for idx, zone in enumerate(zone_maps):
            year_ok = zone["year"]["min"] <= self.year <= zone["year"]["max"]
            month_ok = zone["month"]["min"] <= self.start_month or zone["month"]["max"] >= self.end_month
            town_ok = zone["town"]["min"] <= self.town <= zone["town"]["max"]
            if year_ok and month_ok and town_ok:
                relevant_zones.append(idx)
        return relevant_zones
    
    def calculate_stats(self, zone_indexes):
        individual_stats = self.get_individual_stats(zone_indexes)
        s = None

        def pooled_standard_deviation(n1, s1, n2, s2):
            # Calculate the pooled variance
            variance1 = s1**2
            variance2 = s2**2
            pooled_variance = ((n1 - 1) * variance1 + (n2 - 1) * variance2) / (n1 + n2 - 2)

            # Calculate the pooled standard deviation
            pooled_std_dev = math.sqrt(pooled_variance)

            return pooled_std_dev
        
        for new_s in individual_stats:
            if new_s is None:
                continue
            if s:
                n, n_new = s["num_data"], new_s["num_data"]
                s["num_data"] = n + n_new

                if "area" in self.query:
                    a_min, a_avg, a_std, = s["area_min"], s["area_avg"], s["area_std"]
                    a_min_new, a_avg_new, a_std_new = new_s["area_min"], new_s["area_avg"], new_s["area_std"]
                    s["area_min"] = min(a_min, a_min_new)
                    s["area_avg"] = (a_avg * n + a_avg_new * n_new) / (n + n_new)
                    s["area_std"] = pooled_standard_deviation(n, a_std, n_new, a_std_new)
                
                if "price" in self.query:
                    p_min, p_avg, p_std, = s["price_min"], s["price_avg"], s["price_std"]
                    p_min_new, p_avg_new, p_std_new = new_s["price_min"], new_s["price_avg"], new_s["price_std"]
                    s["price_min"] = min(p_min, p_min_new)
                    s["price_avg"] = (p_avg * n + p_avg_new * n_new) / (n + n_new)
                    s["price_std"] = pooled_standard_deviation(n, p_std, n_new, p_std_new)
            else:
                s = new_s

        return s
            
    def get_individual_stats(self, zone_indexes):
        individual_stats = []
        for zone_idx in zone_indexes:
            validity = self.check_valid_zone(zone_idx)
            # if the zone satify all conditions, we can directly use zone statistics
            if all(validity):
                s = self.zone_maps[zone_idx]
                stats = {
                    "area_min": s["floor_area_sqm"]["min"],
                    "area_avg": s["floor_area_sqm"]["avg"],
                    "area_std": s["floor_area_sqm"]["std"],
                    "price_min": s["resale_price"]["min"],
                    "price_avg": s["resale_price"]["avg"],
                    "price_std": s["resale_price"]["std"],
                    "num_data": s["record_count"]
                }
            else:
                stats = self.read_and_get_stats(zone_idx, validity)
            individual_stats.append(stats)
        return individual_stats
    
    def check_valid_zone(self, zone_idx):
        zone_stats = self.zone_maps[zone_idx]
        town_ok = zone_stats["town"]["min"] == self.town == zone_stats["town"]["max"]
        year_ok = zone_stats["year"]["min"] == self.year == zone_stats["year"]["max"]
        month_ok = zone_stats["month"]["min"] >= self.start_month and zone_stats["month"]["max"] <= self.end_month
        return town_ok, year_ok, month_ok
    
    def read_and_get_stats(self, zone_idx, validity):
        # try to utilize validity to reduce the number of files to read
        town_ok, year_ok, month_ok = validity
        # may use start index and end index if the result is certainly continuous
        zone_stats = self.zone_maps[zone_idx]
        valid_indexes = list(range(zone_stats["index_min"], zone_stats["index_max"]))
        if not town_ok:
            valid_indexes = self.filter_idx(zone_idx, valid_indexes, by="town")
        if not year_ok:
            valid_indexes = self.filter_idx(zone_idx, valid_indexes, by="year")
        if not month_ok:
            valid_indexes = self.filter_idx(zone_idx, valid_indexes, by="month")

        area_data = self.read_data(zone_idx, "floor_area_sqm", valid_indexes)
        price_data = self.read_data(zone_idx, "resale_price", valid_indexes)

        return self.get_stats(area_data, price_data)
    
    def filter_idx(self, zone_idx, valid_indexes, by):
        assert by in ["town", "year", "month"], f"filter by {by} not implemented"
        file_path = self.storage_manager.store_paths[by][zone_idx]
        new_valid_indexes = []
        with open(file_path, "r", newline='') as file:
            print(f"reading file {file_path}")
            reader = csv.DictReader(file)
            row = self.storage_manager.preprocess_row(next(reader), "dict")
            pointer = 0
            end = False
            while pointer<len(valid_indexes):
                # skip rows that are not in current valid_indexes
                while row["index"] != valid_indexes[pointer]:
                    if row["index"] < valid_indexes[pointer]:
                        try:
                            row = self.storage_manager.preprocess_row(next(reader), "dict")
                        except StopIteration:
                            print(f"End reading file: {file_path}")
                            end = True
                            break
                    else:
                        pointer += 1
                        if pointer == len(valid_indexes):
                            end = True
                            break
                # if either valid_index or file is exhausted.
                if end:
                    break

                if self.check_valid(row[by], by):
                    new_valid_indexes.append(row["index"])
                row = self.storage_manager.preprocess_row(next(reader), "dict")

        return new_valid_indexes
                
    def check_valid(self, value, by):
        if by == "year":
            return self.year == value
        elif by == "town":
            return self.town == value
        else:
            return self.start_month <= value <= self.end_month
        
    def read_data(self, zone_idx, col, valid_indexes):
        if "price" in self.query and "price" not in col:
            return None
        elif "area" in self.query and "area" not in col:
            return None
    
        file_path = self.storage_manager.store_paths[col][zone_idx]
        data = []
        with open(file_path, "r", newline='') as file:
            print(f'reading file {file_path}')
            reader = csv.DictReader(file)
            pointer = 0
            end = False
            row = self.storage_manager.preprocess_row(next(reader), "dict")
            while pointer<len(valid_indexes):
                # skip rows that are not in current valid_indexes
                while row["index"] != valid_indexes[pointer]:
                    if row["index"] < valid_indexes[pointer]:
                        try:
                            row = self.storage_manager.preprocess_row(next(reader), "dict")
                        except StopIteration:
                            print(f"End reading file: {file_path}")
                            end = True
                            break
                    else:
                        pointer += 1
                        if pointer == len(valid_indexes):
                            end = True
                            break
                # if either valid_index or file is exhausted.
                if end:
                    break

                data.append(row[col])
                row = self.storage_manager.preprocess_row(next(reader), "dict")
        return data
        
    def get_stats(self, area_data: List[int], price_data: List[int]):
        """Calculate min, average and standard deviation"""
        if not area_data and not price_data:
            return None
        stats = {}
        if area_data:
            stats["num_data"] = len(area_data)
            stats["area_min"] = min(area_data)
            stats["area_avg"] = sum(area_data) / len(area_data)
            stats["area_std"] = (sum((x - stats["area_avg"]) ** 2 for x in area_data) / (stats["num_data"]-1)) ** 0.5
        else:
            stats["num_data"] = len(price_data)
            stats["price_min"] = min(price_data)
            stats["price_avg"] = sum(price_data) / len(price_data)
            stats["price_std"] = (sum((x - stats["price_avg"]) ** 2 for x in price_data) / (stats["num_data"]-1)) ** 0.5

        return stats
    
    def write_results(self, stats):
        """Write results to file"""
        # Check if results folder exists
        if not os.path.exists(RESULTS_FOLDER):
            os.makedirs(RESULTS_FOLDER)
        
        result = {
            "Year":self.year,
            "Month":self.start_month,
            "Town":self.town,
            "Category":self.query,
            "Value": "{:.2f}".format(stats[KEY_MAPPING[self.query]])
        }
        
        field_names = ['Year', 'Month', 'Town', 'Category', 'Value']
        result_path = f'{RESULTS_FOLDER}/ScanResult_{self.matric_num}.csv'
        if not os.path.exists(result_path):
            with open(result_path, "w", newline='') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()

        with open(result_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=field_names)
            writer.writerow(result)