import multiprocessing
import unittest

from es_retriever.config import Config


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.es_url = 'es_url'
        self.es_user = 'bestuser'
        self.es_pass = 'bestuser'
        self.es_base_index = 'bestuser'
        self.es_index_type = 'bestuser'
        self.es_write_url = 'es_write_url'
        self.spark_master_local_cpu_count = 'local[{}]'.format(
            multiprocessing.cpu_count()
        )
        self.es_read_conf = {
            'es.nodes': self.es_url,
            'es.port': '443',
            'es.net.http.auth.user': self.es_user,
            'es.net.http.auth.pass': self.es_pass,
            'es.net.ssl': 'true',
            'es.nodes.wan.only': 'true',
            'es.read.metadata': 'true',
            'es.scroll.size': '1000',
            'es.pushdown': 'true',
            'es.net.proxy.http.host': self.es_url,
            'es.net.proxy.http.port': '443',
            'es.resource': '{}*/{}'.format(
                self.es_base_index, self.es_index_type
            ),
            'es.read.field.as.array.include': 'true',
        }

    def test_instance(self):
        config = Config()
        self.assertEqual(config.es_url, 'localhost:9200')
        self.assertEqual(config.es_user, '')
        self.assertEqual(config.es_pass, '')
        self.assertEqual(config.es_base_index, '')
        self.assertEqual(config.es_index_type, '')
        self.assertEqual(config.es_write_url, None)
        self.assertEqual(config.spark_master, self.spark_master_local_cpu_count)

        config.es_user = self.es_user
        config.es_pass = self.es_pass
        config.es_base_index = self.es_base_index
        config.es_index_type = self.es_index_type
        config.es_url = self.es_url

        self.assertDictEqual(config.es_read_conf, self.es_read_conf)
