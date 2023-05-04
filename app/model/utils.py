from typing import Dict, Any
from datetime import timedelta, datetime as dt

def sap_analyzer_validator(row: Dict):
    try:
        if int(row.get('Total no notifs', 0)) == 1: return True
        else: return False
    except:
        return False

def vas_normalizer(field_name: str, field_value: Any):
    DATE_FIELDS = ['Time', 'Prebratie SAP', 'Prebratie terminál'
                   , 'Uzatvorenie SAP', 'Ukoncenie VAS', ]
    # FORCE_STRING_FIELDS = ['Year-month', 'Year-quarter', 'Year-week']
    try:
        # if field_name in FORCE_STRING_FIELDS:
        #     return str(field_value)
        if field_name in DATE_FIELDS and \
            (isinstance(field_value, float) or isinstance(field_name, int)): 
                days = int(field_value)
                fraction = field_value - days
                seconds = round(fraction * 86400)
                dt_obj = dt(1900, 1, 1) + timedelta(days=days - 2, seconds=seconds)
                date_str = dt_obj.isoformat()
                return date_str
        else: return field_value
    except:
        return None