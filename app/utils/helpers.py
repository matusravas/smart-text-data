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
        normalized_row[' '.join(key.strip().split())] = value
    
    return normalized_row