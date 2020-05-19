import argparse
try:
    import ConfigParser as config_parser
except ImportError:
    import configparser as config_parser
import os
import sys
from datetime import datetime
from dateutil.tz import tzutc
from pyspark.sql import functions as F, Column
from es_retriever.config import Config
from es_retriever.es.storage import EsStorage
from es_retriever.helpers.exceptions import EsretrieverUnautorizedAccess, EsretrieverError
import tempfile


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


def main():
    parser = argparse.ArgumentParser(description='Script to retrieve data from'
                                                 ' ElasticSearch using Pyspark')
    parser.add_argument('since', type=valid_date,
                        help='The start date in the format YYYY-MM-DD HH:MM:SS,'
                             ' e.g. 2018-01-01 00:00:00')
    parser.add_argument('until', type=valid_date,
                        help='The end date in the format YYYY-MM-DD HH:MM:SS, '
                             'e.g. 2018-01-02 00:00:00')
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
    parser.add_argument('-o', '--output',
                        help='The full path and name of the file to save the '
                             'result to, defaults to '
                             'current working dir/base_index')
    parser.add_argument('-b', '--banjax', action='store_true',
                        help='Query Banjax logs instead of Deflect logs')
    parser.add_argument('-v', '--verbose', action='store_true',
                    help='Verbose mode')

    args = parser.parse_args()

    file_to_write = args.output if args.output else args.base_index
    if os.path.isfile(file_to_write):
        print("Output File already exist")
        sys.exit(1)

    config_path = os.path.expanduser('~/.opsdash')
    if not os.path.isfile(config_path):
        print("You should create a configuration file manually, "
              "please check the doc")
        sys.exit(1)

    cp = config_parser.ConfigParser()
    cp.read(config_path)

    if args.banjax:
        config = Config(
            'opsdash.deflect.ca',
            cp.get('OpsDash', 'user'),
            cp.get('OpsDash', 'password'),
            "banjax",
            "banjax"
        )
    else:
        config = Config(
            'opsdash.deflect.ca',
            cp.get('OpsDash', 'user'),
            cp.get('OpsDash', 'password'),
            "deflect.log",
            "deflect_access"
        )

    if not args.verbose:
        config.spark_log_level = 'WARN'

    since = args.since
    until = args.until
    # TODO : check that since < until
    arg_filter_conditions = args.filter
    arg_regex_filter_conditions = args.regex_filter
    arg_sql_like_filter_conditions = args.sql_filter

    # create an EsStorage instance
    storage = EsStorage(config)

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

    print('Getting data from {} to {}, filter(s) {}'.format(
            since, until, filter_condition
        )
    )

    # Create temp dir
    tmpdir = tempfile.mkdtemp()

    # get the data for the period of time
    try:
        df = storage.get(
            since,
            until,
            filter_condition=filter_condition,
            str_filter_condition=arg_sql_like_filter_conditions,
        ).persist()

        df.printSchema()

        print('Records number: %i' % df.count())
        # write to a file
        print('Starting data retrieval and saving to %s' % tmpdir)
        df.coalesce(1).write.mode('overwrite').format('json').save(tmpdir)
        print('Copying results in final output file %s' % file_to_write)

        with open(file_to_write, 'w+') as fout:
            for f in os.listdir(tmpdir):
                if f.startswith('part') and f.endswith('.json'):
                    with open(os.path.join(tmpdir, f), 'r') as fin:
                        fout.write(fin.read())
                    os.remove(os.path.join(tmpdir, f))

    except EsretrieverUnautorizedAccess:
        print("Invalid credentials")
    except EsretrieverError as e:
        print("Weird error : %s" % e.message)

    if os.path.isdir('spark-warehouse'):
        os.rmdir('spark-warehouse')


if __name__ == '__main__':
    main()
