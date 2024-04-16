import os

ORIGINAL_DATA_FILE = 'data/ResalePricesSingapore.csv'
COLUMN_STORE_FOLDER = 'col_store'
RESULTS_FOLDER = 'results'
ZONE_SIZE = 10000
TEMP_FILE_SIZE = 20000
MAPPER = {
    'num2town':{
        '0': 'ANG MO KIO',
        '1': 'BEDOK',
        '2': 'BUKIT BATOK',
        '3': 'CLEMENTI',
        '4': 'CHOA CHU KANG',
        '5': 'HOUGANG',
        '6': 'JURONG WEST',
        '7': 'PUNGGOL',
        '8': 'WOODLANDS',
        '9': 'YISHUN',
    },
    'town2num': {
        'ANG MO KIO': 0,
        'BEDOK': 1,
        'BUKIT BATOK': 2,
        'CLEMENTI': 3,
        'CHOA CHU KANG': 4,
        'HOUGANG': 5,
        'JURONG WEST': 6,
        'PUNGGOL': 7,
        'WOODLANDS': 8,
        'YISHUN': 9,
    }
}

RELEVANT_COLS = (
    # these three columns can be adjusted for a different soring order
    'town', 
    'year', 
    'month',

    'floor_area_sqm',
    'resale_price'
)

QUERY_TYPES = [
    "Minimum Area",
    "Average Area",
    "Standard Deviation of Area",
    "Minimum Price",
    "Average Price",
    "Standard Deviation of Price"
]

KEY_MAPPING = {
    "minimum area":"area_min",
    "average area":"area_avg",
    "standard deviation of area":"area_std",
    "minimum price":"price_min",
    "average price":"price_avg",
    "standard deviation of price":"price_std"
}

ORIGINAL_DATA_FILE = os.path.abspath(ORIGINAL_DATA_FILE)
COLUMN_STORE_FOLDER = os.path.abspath(COLUMN_STORE_FOLDER)
RESULTS_FOLDER = os.path.abspath(RESULTS_FOLDER)