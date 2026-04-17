from datetime import datetime, timedelta

EXCEL_BASE = datetime(1899, 12, 30)
LEGALSUITE_OFFSET = 36161


def decode_legalsuite_date(value: int) -> str:
    """
    Convert LegalSuite stored date integer to YYYY-MM-DD
    """
    excel_serial = value - LEGALSUITE_OFFSET
    date_value = EXCEL_BASE + timedelta(days=excel_serial)
    return date_value.strftime("%Y-%m-%d")


def encode_legalsuite_date(date_str: str) -> int:
    """
    Convert YYYY-MM-DD date to LegalSuite integer format
    """
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    excel_serial = (dt - EXCEL_BASE).days
    return excel_serial + LEGALSUITE_OFFSET


# examples
print(decode_legalsuite_date(82281))  # 2026-04-07
#print(encode_legalsuite_date("2026-04-08"))  # 82280
