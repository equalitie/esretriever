import abc
from functools import wraps
try:
    from urllib import quote_plus
except ImportError:
    # python 3
    from urllib.parse import quote_plus
import requests
from pyspark.sql import functions as F
from es_retriever.es import index_date_formatter
from es_retriever.helpers.time_period import TimePeriod, ENDCOLOR
from es_retriever.helpers.exceptions import (EsretrieverUnautorizedAccess,
                                             EsretrieverError)
from es_retriever.spark import get_or_create_spark_session


def get_es_cluster_health(f):

    @wraps(f)
    def wrapper(*args, **kwargs):
        self = args[0]
        q = 'http{s}://{u}:{p}@{n}/{q}'.format(
            s='s' if self.es_read_config['es.net.ssl'] else '',
            u=quote_plus(self.es_read_config['es.net.http.auth.user']),
            p=quote_plus(self.es_read_config['es.net.http.auth.pass']),
            n=self.es_read_config['es.nodes'].replace(
                'http://', ''
            ).replace(
                'https://', ''
            ).rstrip('/'),
            q='_cluster/health'
        )

        print('Please, wait a few seconds while cluster health is checked...')
        # TODO : move all requests to the cluster to another class
        req = requests.get(q)
        if req.status_code != 200:
            if req.status_code == 401:
                raise EsretrieverUnautorizedAccess()
            else:
                raise EsretrieverError(
                    "Invalid HTTP code %i" % req.status_code
                )
        health = req.json()
        c = self.cluster_status_to_color[health['status']]
        print(c + 'Cluster status: {}'.format(health['status']) + ENDCOLOR)

        if health['status'] != 'green':
            raise RuntimeError(
                c + 'Cluster status is not green: {}, no queries should '
                    'be performed'.format(health['status']) + ENDCOLOR
            )
        return f(*args, **kwargs)
    return wrapper


class PySparkStorage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self,
                 config,
                 format,
                 session_getter=get_or_create_spark_session):
        # type: (config.Config, str, function) -> None
        self.format = format
        self.session_getter = session_getter
        self.config = config

    @abc.abstractmethod
    def get(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def save(self, *args, **kwargs):
        pass


class EsStorage(PySparkStorage):
    """
    Wrapper to retrieve data from ElasticSearch using pyspark
    """

    def __init__(self,
                 config,
                 session_getter=get_or_create_spark_session,
                 format='org.elasticsearch.spark.sql',
                 time_formatter=index_date_formatter,
                 init_session=True,
                 timestamp_column='@timestamp'
                 ):
        super(EsStorage, self).__init__(config, format, session_getter)
        self.base_index = config.es_base_index
        self.index_type = config.es_index_type
        self.es_read_config = config.es_read_conf
        self.time_formatter = time_formatter
        self.timestamp_column = timestamp_column

        self.cluster_status_to_color = {
            'green': '\033[92m',
            'yellow': '\033[93m',
            'red': '\033[91m'
        }

        if init_session:
            session_getter(config)

    @property
    def base_resource(self):
        """
        Forms the string that represents the base resource for the ES request
        :return:
        """
        return '{}*/{}'.format(self.base_index, self.index_type)

    def _get_multiday_resource(self, period_of_time):
        """
        Returns the indexes where pyspark will look into to get data, split per
        day in the period of time requested. E.g. for a period of time from
        2018-04-18 12:12:00 to 2018-04-19 13:00:00, it will return
        'base_index-2018.04.18,base_index-2018.04.19/index_type'
        :param TimePeriod period_of_time:the period of time requested
        :rtype: str
        :return: the multiday resource string representation
        """
        return '{}/{}'.format(
            ','.join(
                [self.time_formatter(self.base_index, day.start)
                 for day in period_of_time.split_per_day()]
            ),
            self.index_type
        )

    def _get_es_read_config(self, period_of_time):
        """
        Return the configuration for the specific period of time (so that
        specific indexes are requested)
        :param es_retriever.helpers.TimePeriod period_of_time: the period of
        time to search for in ES
        :rtype: dict[str, T]
        :return: the dictionary that holds the necessary configuration for
        communicating with ES
        """
        self.es_read_config['es.resource'] = self._get_multiday_resource(
            period_of_time
        )
        return self.es_read_config

    @get_es_cluster_health
    def get(self,
            since,
            until,
            filter_condition=None,
            str_filter_condition=None,
            extra_config=None,
            columns_to_keep=None,
            columns_to_expand=None,
            columns_to_drop = None,):
        """
        Get data from ElasticSearch from indexes that hold the data for the
        period of interest, filtered for that period using the timestamp_field.
        :param datetime.datetime since: the start date
        :param datetime.datetime until: the end date
        :param pyspark.sql.Column filter_condition: the predicate that
        contains the spark filtering conditions to be applied on the requested
        dataframe.
        :param str str_filter_condition: sql-like filtering
        dataframe.
        :param: dict[str, str] extra_config: any additional configuration for
        communicating with ES
        :param: columns_to_keep: fields keep in df (keep all if None)
        :param: columns_to_expand: fields to expand in df
        :param: columns_to_drop: fields drop from df (keep all if None)
        :rtype: pyspark.sql.DataFrame
        :return: the dataframe with the data
        """
        # get spark instance
        session = self.session_getter()
        print(f'Spark UI accessible at:{session.sparkContext.uiWebUrl}')
        period_of_time = TimePeriod(since, until)
        config = self._get_es_read_config(period_of_time)
        extra_config = extra_config \
            if extra_config and isinstance(extra_config, dict) \
            else {}
        config.update(extra_config)

        # get data for period of time
        df = session.read.format(self.format) \
            .options(**config) \
            .load(self._get_multiday_resource(period_of_time))

        # filter by since until - for hour filtering
        date_time_filter = (F.col('@timestamp') >= since) & \
                           (F.col('@timestamp') <= until)
        # filter to get only what's in the period
        if filter_condition is not None:
            df = df.filter(filter_condition & date_time_filter)
        else:
            df = df.filter(date_time_filter)

        # filter using sql-like syntax
        if str_filter_condition:
            df = df.filter(str_filter_condition)

        # use all columns if columns_to_keep not specified
        if not columns_to_keep:
            columns_to_keep = df.columns

        # expand nested columns
        if columns_to_expand:
            columns_to_keep = columns_to_keep + columns_to_expand

        # drop unwanted columns
        if columns_to_drop:
            for c in columns_to_drop:
                if c in columns_to_keep:
                    columns_to_keep.remove(c)

        print(columns_to_keep)
        df = df.select(*columns_to_keep)

        print(f"Saved fields: {columns_to_keep},"
              f"n entries: {df.count()}")

        return df

    @get_es_cluster_health
    def save(self):
        raise NotImplementedError('TODO')
