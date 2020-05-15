import os
import multiprocessing


class Config(object):
    spark_log_level = 'INFO'
    spark_local_parallelism = multiprocessing.cpu_count()
    spark_shuffle_partitions = 14
    spark_executor_instances = 2
    spark_executor_cores = 2

    def __init__(
            self,
            es_url='localhost:9200',
            es_user='',
            es_pass='',
            es_base_index='',
            es_index_type='',
            es_write_url=None,
            spark_master='local'
    ):
        self.spark_master = spark_master
        if self.spark_master == 'local' and self.spark_local_parallelism > 0:
            self.spark_master = 'local[{}]'.format(
                self.spark_local_parallelism)
        self.es_url = es_url
        self.es_user = es_user
        self.es_pass = es_pass
        self.es_base_index = es_base_index
        self.es_index_type = es_index_type
        self.es_write_url = es_write_url

        self.__es_read_conf = {
            'es.nodes': self.es_url,
            'es.port': '443',
            'es.net.http.auth.user': self.es_user,
            'es.net.http.auth.pass': self.es_pass,
            'es.net.ssl': 'true',
            'es.nodes.wan.only': 'true',
            'es.read.metadata': 'true',
            # 'es.thread_pool.bulk.queue_size': '100',
            'es.scroll.size': '1000',
            'es.pushdown': 'true',
            'es.net.proxy.http.host': self.es_url,
            'es.net.proxy.http.port': '443',
            'es.resource': '{}*/{}'.format(
                self.es_base_index, self.es_index_type
            ),
            'es.read.field.as.array.include': 'true',
        }

        self.__spark_conf = {
            'spark.sql.session.timeZone': 'UTC',
            'spark.sql.shuffle.partitions': self.spark_shuffle_partitions,
            'spark.executor.instances': self.spark_executor_instances,
            'spark.executor.cores': self.spark_executor_cores,
            'spark.logConf': 'true',
            'spark.jars': os.path.abspath(os.path.dirname(__file__)) +
                          '/jars/elasticsearch-spark-20_2.11-5.6.5.jar',
        }

        self.__es_write_conf = {
            'es.nodes': self.es_write_url,
            # 'es.thread_pool.bulk.queue_size': '100',
            'es.scroll.size': '1000',
            'es.pushdown': 'true',
            'es.resource': '{}*/{}'.format(
                self.es_base_index, self.es_index_type
            ),
            'es.read.field.as.array.include': 'true',
        }

    def _check_type(self, attr, value, type_=dict):
        if isinstance(value, type_):
            setattr(self, attr, value)
        else:
            raise TypeError('{} must be of type {}'.format(attr, type_))

    @property
    def es_read_conf(self):
        return self.__es_read_conf

    @es_read_conf.setter
    def es_read_conf(self, value):
        self._check_type('__es_read_conf', value)

    @property
    def spark_conf(self):
        return self.__spark_conf

    @spark_conf.setter
    def spark_conf(self, value):
        self._check_type('__spark_conf', value)

    @property
    def es_write_conf(self):
        return self.__es_write_conf

    @es_write_conf.setter
    def es_write_conf(self, value):
        self._check_type('__es_write_conf', value)


if __name__ == '__main__':
    conf = Config()

    conf.es_read_conf = {}
    try:
        conf.es_read_conf = 1
    except TypeError as e:
        print(e)

    conf.es_url = 'test_url'
    print(conf.es_read_conf)
