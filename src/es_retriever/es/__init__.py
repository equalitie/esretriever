def index_date_formatter(base, date):
    """
    Returns a time based index representation for the given base, date
    :param str base: the index's base name
    :param datetime.datetime date:the date to format the index for
    :rtype: str
    :return: the index representation for the given date
    """
    return '{b}-{y}.{m}.{d}'.format(
             b=base,
             y=date.year,
             m=str(date.month).zfill(2),
             d=str(date.day).zfill(2),
         )
