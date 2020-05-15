import os

from es_retriever.spark import get_or_create_spark_session
from es_retriever.config import Config
import pyspark.sql.functions as F

# NOTE: use this script to save time-based logs to *local* elasticsearch.
# NOT to be used against the cluster

ES_VERSION = '5.6.5'
jar_path = os.path.abspath(os.path.dirname(__file__)) + \
           '/../jars/elasticsearch-spark-20_2.11-{}.jar'.format(ES_VERSION)
logs_json_path = 'path/to/oldlogs'
timestamp_field = '@timestamp'
base_index_name = 'oldlogs'
base_index_type = '_doc'

# for daily based logs
write_template = '{index}-{{{ts_field}|YYYY.MM.dd}}/{type}'.format(
        index=base_index_name,
        ts_field=timestamp_field,
        type=base_index_type
    )

spark_conf = {
    'spark.sql.session.timeZone': 'UTC',
    'spark.sql.shuffle.partitions': 14,
    'spark.executor.instances': 4,
    'spark.executor.cores': 4,
    'spark.logConf': 'true',
    'spark.jars': jar_path
}
es_conf = {
    'es.nodes': 'localhost',
    'es.read.metadata': 'true',
    'es.scroll.size': '1000',
    'es.pushdown': 'true',
    'es.net.http.auth.user': 'elastic',   # use for ES version 5.x
    'es.net.http.auth.pass': 'changeme',  # -- // --
    'es.nodes.wan.only': 'true',
    'es.resource': '{}*/{}'.format(base_index_name, base_index_type),
    'es.resource.write': write_template
}

if __name__ == '__main__':
    # set configuration and create spark session
    config = Config()
    config.spark_conf = spark_conf
    config.es_read_conf = es_conf
    spark = get_or_create_spark_session(config)

    # read from logs
    df = spark.read.json(logs_json_path)

    # due to a bug in the elasticsearch hadoop connector, the date needs to be
    # cast to timestamp and stringified in order for the pattern write
    # to succeed
    df = df.withColumn(
        timestamp_field, F.col(timestamp_field).cast('timestamp')
    )
    df = df.withColumn(
        timestamp_field,
        F.date_format(df[timestamp_field], "yyyy-MM-dd'T'HH:mm:ss")
    )

    df.show()

    df.write.mode(
        'overwrite'
    ).format(
        'org.elasticsearch.spark.sql'
    ).options(
        **es_conf
    ).save(write_template)
