from datetime import datetime


def get_date_YYYY_MM_DD():
    now = datetime.now()
    date = now.strftime("%Y-%m-%d")
    return date
