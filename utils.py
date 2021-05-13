import datetime as dt

def parse_iso_date(date_str):
    return dt.datetime.strptime(date_str, '%Y-%m-%d').date()

def format_iso_date(date):
    return date.strftime('%Y-%m-%d')