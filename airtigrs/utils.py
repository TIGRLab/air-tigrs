import datetime


def as_datetime(mtime):
    return datetime.datetime.fromtimestamp(mtime).strftime('%Y%m%d%H%M%S')
