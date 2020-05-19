import os
import argparse

from datetime import datetime
from getpass import getpass

from dateutil.tz import tzutc
from es_retriever.helpers.time_period import TimePeriod
from pyspark.sql import functions as F, Column

from es_retriever.config import Config
from es_retriever.es.storage import EsStorage


def get_es_jar_path():
    """
    Returns the absolute path to the data folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../jars/' \
           f'elasticsearch-spark-20_2.11-5.6.5.jar'


def valid_date(s, pattern="%Y-%m-%d %H:%M:%S"):
    """
    Try to parse a string s to datetime
    :param str s: a string representation of datetime
    :rtype: datetime
    :return: the datetime representation of s
    """
    try:
        return datetime.strptime(s, pattern).replace(tzinfo=tzutc())
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def validate_path(full_path_to_file):
    """
    Check if path that leads to full_path_to_file exists
    :param str full_path_to_file: path with file name
    :return: full_path_to_file if path is valid, raises ArgumentError otherwise
    """
    import os

    path_without_file = '/'.join(full_path_to_file.split('/')[:-1])
    if not os.path.exists(path_without_file):
        msg = "Not a valid path for: '{0}'.".format(full_path_to_file)
        raise argparse.ArgumentError(msg)
    return full_path_to_file


def aggregate_conditions(prev, next):
    if prev:
        return [prev[-1] & next]
    return [next]


def get_filter_conditions(arg_filter_conditions, is_regex=False):
    """
    Aggregates the argument provided filter conditions if any.
    NOTE: The aggregation assumes the AND operator.
    :param list[str] arg_filter_conditions: list of filter condition pairs
    :param boolean is_regex: True if dealing with regex conditions, False
    otherwise. Default is False.
    :rtype: pyspark.sql.Column
    :return: the column conditions aggregated.
    """
    if arg_filter_conditions:
        filter_conditions = []

        for filter_pair in arg_filter_conditions:
            field, condition = filter_pair.split('=')

            col_condition = (F.col(field).rlike(condition)) \
                if is_regex else (F.col(field) == condition)

            filter_conditions = aggregate_conditions(
                filter_conditions, col_condition
            )
        filter_conditions = filter_conditions[0]
    else:
        filter_conditions = None

    return filter_conditions


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to retrieve data from'
                                                 'ElasticSearch using Pyspark')
    parser.add_argument('elasticsearch', type=str, default='localhost',
                        help='The elasticsearch url, defaults to localhost')
    parser.add_argument('base_index', type=str,
                        help='The index name without the date part e.g. '
                             'deflect.log or banjax')
    parser.add_argument('index_type', type=str,
                        help='The index type field, e.g. deflect_access '
                             'or banjax')
    parser.add_argument('since', type=valid_date,
                        help='The start date in the format %Y-%m-%d %H:%M:%S, '
                             'e.g. 2018-01-01 00:00:00')
    parser.add_argument('until', type=valid_date,
                        help='The end date in the format %Y-%m-%d %H:%M:%S, '
                             'e.g. 2018-01-02 00:00:00')
    parser.add_argument('-u', '--username', type=str,
                        help='The username that has access directly to '
                             'ElasticSearch')
    parser.add_argument('-p', '--password', action='store_true',
                        dest='password', help='The username\'s password')
    parser.add_argument('-f', '--filter', type=str, nargs='*',
                        help='extra filters separated by space, e.g. '
                             'to get data for a specific dnet and a specific '
                             'ip, use dnet=somednet client_ip=95.11.1.111')
    parser.add_argument('-rf', '--regex_filter', type=str,
                        nargs='*',
                        help='extra regex filters separated by space, e.g. '
                             'dnet="some_valid_regex" '
                             'client_ip="95\.11\.\d{1,3}\.111"')
    parser.add_argument('-sf', '--sql_filter', type=str,
                        help='SQL like filter for more complex quering, e.g. '
                             'dnet="somednet" OR day-of-week>=2')
    parser.add_argument('-o', '--output', type=validate_path,
                        help='The full path and name of the file to save the '
                             'result to, defaults to '
                             'current working dir/base_index')

    args = parser.parse_args()

    password = None
    if args.password:
        password = getpass()

    config = Config(
        args.elasticsearch,
        args.username,
        password,
        args.base_index,
        args.index_type
    )

    # todo: modify config to accompodate this
    config.spark_conf['spark.jars'] = get_es_jar_path()

    since = args.since
    until = args.until
    arg_filter_conditions = args.filter
    arg_regex_filter_conditions = args.regex_filter
    arg_sql_like_filter_conditions = args.sql_filter

    # create an EsStorage instance
    storage = EsStorage(
        config
    )

    # get filters if any
    filter_conditions = get_filter_conditions(arg_filter_conditions)
    regex_filter_conditions = get_filter_conditions(
        arg_regex_filter_conditions, True
    )
    # combine them
    if isinstance(filter_conditions, Column):
        if isinstance(regex_filter_conditions, Column):
            filter_condition = (filter_conditions) & (regex_filter_conditions)
        else:
            filter_condition = filter_conditions
    else:
        filter_condition = None

    tw = TimePeriod(since, until)

    active_columns = 'client_ip,client_request_host,querystring,' \
                     'content_type,@timestamp,http_response_code,' \
                     'client_ua,client_url,reply_length_bytes'

    days_tw = tw.split_per_day(full_day=True)
    print(f'Splitting period into {len(days_tw)} day(s)')
    for day_tw in days_tw:
        print('Getting data from {} to {}, filter(s) {}'.format(
            day_tw.start, day_tw.end, filter_condition
        ))
        # get the data for the period of time
        df = storage.get(
            day_tw.start,
            day_tw.end,
            filter_condition=filter_condition,
            str_filter_condition=arg_sql_like_filter_conditions,
            extra_config={
                'es.mapping.include': active_columns
            },
            columns_to_keep=active_columns.split(',')
        ).persist()

        df.printSchema()

        print('Records number:', df.count())
        # write to a file
        file_to_write = f'{args.output if args.output else args.base_index}_' \
                        f'{str(day_tw.start.date())}-{str(day_tw.end.date())}'
        print('Starting data retrieval and saving to {}'.format(file_to_write))
        # df.coalesce(1).write.mode('overwrite').json(file_to_write)
        df.coalesce(1).write.mode('overwrite').format('json').save(
            file_to_write
        )
