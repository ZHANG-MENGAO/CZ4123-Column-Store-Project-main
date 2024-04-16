# CZ4123-Project

Steps to get started:

1. `python -m venv venv`
2. `.venv/Scripts/activate`(Windows) or `source .venv/bin/activate`(Linux)
3. `pip install -r requirements.txt`
5. `python src/main.py`

Project Idea:
- First, sort the data by `month` column and break data into smaller zones and store in files. Each file contain only a subset of a column. For example, `month` column is broken into `month_0.txt`, `month_1.txt`, etc and town column is broken into `town_0.txt`, `town_1.txt`, etc.
- Create a zone map that store minimum and maximum value for each files so the search can be speed up later. Because the data is sorted by month column, technically the zone map only need the month column value to search faster.
- When receive a query, I first use the zone map to know which zones I should search for (the data can fall into two different zones). 
- Use binary search on `month` column to filter out the data out of the query month range.
- Filter out the data by the query town.
- Use the corresponding filtered index to get the price and area to calculate min, avg, std.