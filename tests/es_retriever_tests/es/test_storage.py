from unittest import mock

import pyspark.sql.functions as F
from datetime import datetime

from dateutil.tz import tzutc
from undecorated import undecorated
from sparktestingbase.sqltestcase import SQLTestCase


class TestEsStorage(SQLTestCase):
    def setUp(self):
        super(TestEsStorage, self).setUp()
        self.patcher = mock.patch('es_retriever.spark.get_or_create_spark_session')
        self.mock_session = self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    def test_instance(self):
        from es_retriever.es.storage import EsStorage
        from es_retriever.config import Config

        self.config = Config()
        es_storage = EsStorage(
            self.config,
            session_getter=self.mock_session
        )

        self.assertTrue(hasattr(es_storage, 'base_index'))
        self.assertTrue(hasattr(es_storage, 'index_type'))
        self.assertTrue(hasattr(es_storage, 'es_read_config'))
        self.assertTrue(hasattr(es_storage, 'time_formatter'))
        self.assertTrue(hasattr(es_storage, 'timestamp_column'))
        self.assertTrue(hasattr(es_storage, 'cluster_status_to_color'))

        self.assertEqual(es_storage.format, 'org.elasticsearch.spark.sql')
        self.assertEqual(es_storage.session_getter, self.mock_session)
        self.assertEqual(es_storage.config, self.config)

    def test_get_no_filter_condition(self):
        from es_retriever.es.storage import EsStorage
        from es_retriever.config import Config

        self.config = Config(
            es_base_index='test_index',
            es_index_type='test_type'
        )
        es_storage = EsStorage(
            self.config,
            session_getter=self.mock_session
        )
        since = datetime(2018, 1, 1, 10, 35, 00).replace(tzinfo=tzutc())
        until = datetime(2018, 1, 2, 13, 35, 00).replace(tzinfo=tzutc())

        undecorated(es_storage.get)(es_storage, since, until)

        format = self.mock_session.return_value.read.format
        options = format.return_value.options
        load = options.return_value.load
        df_ = load.return_value
        format.assert_called_once_with(
            'org.elasticsearch.spark.sql'
        )
        options.assert_called_once_with(
            **es_storage.es_read_config
        )
        load.assert_called_once_with(
            'test_index-2018.01.01,test_index-2018.01.02/test_type'
        )

        df_.filter.assert_called_once()
        # df_ = df_.filter.return_value
        columns = df_.columns

        df_.filter.return_value.select.assert_called_once_with(columns)

    def test_get_with_filter_condition(self):
        from es_retriever.es.storage import EsStorage
        from es_retriever.config import Config

        mock_filter = mock.MagicMock()

        self.config = Config(
            es_base_index='test_index',
            es_index_type='test_type'
        )
        es_storage = EsStorage(
            self.config,
            session_getter=self.mock_session,
        )
        since = datetime(2018, 1, 1, 10, 35, 00).replace(tzinfo=tzutc())
        until = datetime(2018, 1, 2, 13, 35, 00).replace(tzinfo=tzutc())
        date_time_filter = (F.col('@timestamp') >= since) & \
                           (F.col('@timestamp') <= until)

        undecorated(es_storage.get)(
            es_storage,
            since,
            until,
            filter_condition=mock_filter
        )

        format = self.mock_session.return_value.read.format
        options = format.return_value.options
        load = options.return_value.load
        df_ = load.return_value
        format.assert_called_once_with(
            'org.elasticsearch.spark.sql'
        )
        options.assert_called_once_with(
            **es_storage.es_read_config
        )
        load.assert_called_once_with(
            'test_index-2018.01.01,test_index-2018.01.02/test_type'
        )

        df_.filter.assert_called_once()
        df_.filter.assert_called_once_with(mock_filter & date_time_filter)
        df_ = df_.filter.return_value
        columns = df_.columns

        df_.select.assert_called_once_with(columns)

    def test_get_with_extra_config(self):
        from es_retriever.es.storage import EsStorage
        from es_retriever.config import Config

        extra_config = {'test1': 'test_value1'}

        self.config = Config(
            es_base_index='test_index',
            es_index_type='test_type'
        )
        es_storage = EsStorage(
            self.config,
            session_getter=self.mock_session,
        )
        since = datetime(2018, 1, 1, 10, 35, 00).replace(tzinfo=tzutc())
        until = datetime(2018, 1, 2, 13, 35, 00).replace(tzinfo=tzutc())

        undecorated(es_storage.get)(
            es_storage,
            since,
            until,
            extra_config=extra_config
        )

        format = self.mock_session.return_value.read.format
        options = format.return_value.options
        load = options.return_value.load
        df_ = load.return_value

        config = es_storage.es_read_config.copy()
        config.update(extra_config)

        format.assert_called_once_with(
            'org.elasticsearch.spark.sql'
        )
        options.assert_called_once_with(
            **config
        )
        load.assert_called_once_with(
            'test_index-2018.01.01,test_index-2018.01.02/test_type'
        )

        df_.filter.assert_called_once()
        df_ = df_.filter.return_value
        columns = df_.columns

        df_.select.assert_called_once_with(columns)
