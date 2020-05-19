from datetime import datetime
from pyspark.sql import functions as F

from es_retriever.config import Config
from es_retriever.es.storage import EsStorage


if __name__ == '__main__':
    config = Config(
        'HOST', 'USERNAME', 'PASSWORD', 'INDEX', 'TYPE'
    )
    since = datetime(2018, 4, 11, 10, 00, 00)
    until = datetime(2018, 4, 11, 10, 10, 00)

    # create an EsStorage instance
    storage = EsStorage(
        config
    )

    # get only one hour for this day
    filter_condition = (F.col('@timestamp') >= since) & \
                       (F.col('@timestamp') < until)

    # get the data for the period of time
    df = storage.get(
        since,
        until,
        filter_condition=filter_condition
    ).persist()

    df.printSchema()

    # write to a file
    df.write.mode('overwrite').json(storage.base_resource)
