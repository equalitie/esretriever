from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_conf(spark_conf):
    """

    :param dict[str, T] extra_conf:
    :rtype: pyspark.SparkConf
    :return: returns an instance of SparkConf with the parameters set
    """
    conf = SparkConf()

    for k, value in spark_conf.items():
        conf.set(k, value)

    return conf


def get_or_create_spark_session(conf=None):
    """
    Returns the spark session
    :param Config conf: the configuration instance
    :return: the configured spark session
    :rtype: pyspark.sql.SparkSession
    """
    if not conf:
        # only use after spark has been instantiated
        return SparkSession.builder.getOrCreate()

    spark = SparkSession.builder \
        .config(conf=get_spark_conf(conf.spark_conf)) \
        .master(conf.spark_master) \
        .appName('Let\' not break ES') \
        .getOrCreate()

    if conf.spark_conf.get('spark.logConf', None) == 'true':
        spark.sparkContext.setLogLevel(conf.spark_log_level)

    return spark
