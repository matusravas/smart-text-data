from typing import Any
import pandas as pd


def parse_field(field: Any):
    #! Todo handle NaT and nan fields !
    if isinstance(field, pd.Timestamp): return field.isoformat()
    elif pd.isnull(field): return None
    else: return field