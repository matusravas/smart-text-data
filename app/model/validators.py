from typing import Dict


def sap_analyzer_validator(row: Dict):
    try:
        if int(row.get('Total no notifs', 0)) == 1: return True
        else: return False
    except:
        return False