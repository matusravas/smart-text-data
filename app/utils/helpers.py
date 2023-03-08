from typing import Dict
import pandas as pd
from datetime import datetime as dt

from app.model import NULL_VALUES

def normalize_row(row: Dict):
    normalized_row = {}
    for key, value in row.items():
        if value in NULL_VALUES or pd.isnull(value) or pd.isna(value):
            value = None
        elif isinstance(value, pd.Timestamp) or isinstance(value, dt): 
            value = value.isoformat()  
        normalized_row[key] = value
    
    return normalized_row

# def parse_field(field: Any):
#     if pd.isnull(field) or pd.isna(field): 
#         return None
#     elif isinstance(field, pd.Timestamp) or isinstance(field, dt): 
#         return field.isoformat()
#     else: return field