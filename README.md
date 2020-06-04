# Retrieve Logs from ElasticSearch using Spark

## Pre-requisites

- Python 3
- Java 8+ needs to be in place (and in PATH) for Spark (Pyspark version 2.2+) to work [src](https://spark.apache.org/docs/2.2.0/)

## Set-up

- Create a virtual env and activate it with `source /path/to/venv/bin/activate`
- `cd /path/to/setup.py`
- `pip install -e .` to install `es-retriever` (note: `-e` flag is important to be able to change the configuration. This will be changed in the future)

### Run tests
`python -m pytest`

## Usage

NOTE: While the script is running, `localhost:4040` displays information about what
spark does.

### Retrieving data

Under `es-pyspark-retriever/src/es_retriever/examples` there is a [simple example](/src/es_retriever/examples/simple_retrieve.py) retrieving data for one day for a single hour and an [example](/src/es_retriever/examples/week_retrieve.py) that retrieves data for one week.

Configuration information should be given as parameters when initializing the `Config` object, for instance `Config('localhost', 'user', 'password', 'deflect.log', 'deflect_access')`

#### Common part in all examples is the creation of the Config and EsStorage instances:
```python
from datetime import datetime

from es_retriever.config import Config
from es_retriever.es.storage import EsStorage


# create a configuration instance
config = Config(SERVER, 'user', 'password', 'deflect.log', 'deflect_access')

# create an EsStorage instance with the given configuration
storage = EsStorage(
   config
)
```

#### Common - storing data

```python
# note: the following will create multiple files - fileparts
# write to a file as json
df.write.mode('overwrite').json('somefilename')
# or as csv
df.write.mode('overwrite').csv('somefilename')
```

To save to a **single** file instead of multiparts:
```
df.coalesce(1).write.mode('overwrite').json('full/path/to/file')
```

Mode can be:
    * `append`: Append contents to existing data.
    * `overwrite`: Overwrite existing data.
    * `error` or `errorifexists`: Throw an exception if data already exists.
    * `ignore`: Silently ignore this operation if data already exists.

#### Example #1 get data for a day

Get data for 1 day from `11-04-18` to `12-4-18`:

```python
# the dates we need to look for
since = datetime(2018, 4, 11)
until = datetime(2018, 4, 12)

# get the data for the period of time
df = storage.get(since, until)
```

#### Example #2 get data for an hour of a day
If hours / minutes / seconds are specified in since / until, the filtering of the data
will take that into account.
```python
# the dates we need to look for
since = datetime(2018, 4, 11, 10, 00, 00)
until = datetime(2018, 4, 11, 11, 00, 00)

# get the data for the period of time
df = storage.get(since, until)

```

#### Example #3 get data for an hour of a day (using hour-of-day field in logs)

```python
# the dates we need to look for
since = datetime(2018, 4, 11, 10, 00, 00)
until = datetime(2018, 4, 11, 11, 00, 00)

# hour filter:
hour_filter = (
(F.col('@timestamp') >= since) & (F.col('@timestamp') <= until)
)
# get the data for the period of time
df = storage.get(since, until, hour_filter)

# or use the hour-of-day field in the logs:
hour_filter = (
(F.col('hour-of-day') >= 10) & (F.col('hour-of-day') <= 11)
)

# get the data for the period of time
df = storage.get(since, until, hour_filter)
```

#### Example #4 get data for a week
```python
# the dates we need to look for
since = datetime(2018, 4, 11)
until = datetime(2018, 4, 18)

# get the data for the period of time
df = storage.get(since, until)

```
#### Example #5 get data for a week - one hour for each day
##### One way - have every day saved in a different file:
```python
# define the start day: 01-04-2018
since = datetime(2018, 4, 1, 10, 00, 00)
until = datetime(2018, 4, 1, 11, 00, 00)

# iterate for a week
for day_no in range(7):
    # keep the name for convenience
    file_name = '{}-{year}.{month}.{day}.{hour}.{minute}.{second}'.format(
        config.es_base_index,
        year=since.year,
        month=str(since.month).zfill(2),
        day=str(since.day).zfill(2),
        hour=str(since.hour).zfill(2),
        minute=str(since.minute).zfill(2),
        second=str(since.second).zfill(2)
    )

    # get the data for the period of time
    df = storage.get(
        since,
        until
    )

    print 'Retrieving data for {}'.format(file_name)

    # save
    df.write.mode('overwrite').json(file_name)

    # increment since and until by one day
    since = since + timedelta(days=1)
    until = until + timedelta(days=1)

```

##### A simpler way - have all days saved in one file:
```python
# define the start day:
since = datetime(2018, 4, 1)
until = datetime(2018, 4, 7)

# get only 10 to 11 am
hour_filter = (
(F.col('hour-of-day') >= 10) & (F.col('hour-of-day') <= 11)
)

# get the data for the period of time
df = storage.get(
    since,
    until,
    hour_filter
)

# save
df.write.mode('overwrite').json('data_for_week_somenumber')
```

A bit more complex hour filter:
```python
# define the start day:
since = datetime(2018, 4, 1)
until = datetime(2018, 4, 7)

# get only 10 to 11 am and 15 to 16 pm
hours_filter = (
(F.col('hour-of-day') >= 10) & (F.col('hour-of-day') <= 11) &
(F.col('hour-of-day') >= 15) & (F.col('hour-of-day') <= 16)
)

# get the data for the period of time
df = storage.get(
    since,
    until,
    hours_filter
)
```

#### Example #6 regex filters:

To filter the rows where some field matches a regex expression:
```
df = df.filter(df["field / column name"].rlike('regex expression'))
```

#### CLI example

When the es-pyspark-retriever package is installed, it also installs an `esretrieve` command. To use this tool, you first need to create a configuration file in `~/opsdash` with the following format :
```
[OpsDash]
user: USERNAME HERE
password: PASSWORD HERE
```

The options so far:
```
Script to retrieve data fromElasticSearch using Pyspark

positional arguments:
  since                 The start date in the format YYYY-MM-DD HH:MM:SS, e.g.
                        2018-01-01 00:00:00
  until                 The end date in the format YYYY-MM-DD HH:MM:SS, e.g.
                        2018-01-02 00:00:00

optional arguments:
  -h, --help            show this help message and exit
  -f [FILTER [FILTER ...]], --filter [FILTER [FILTER ...]]
                        extra filters separated by space, e.g. to get data for
                        a specific dnet and a specific ip, use dnet=somednet
                        client_ip=95.11.1.111
  -rf [REGEX_FILTER [REGEX_FILTER ...]], --regex_filter [REGEX_FILTER [REGEX_FILTER ...]]
                        extra regex filters separated by space, e.g.
                        dnet="some_valid_regex"
                        client_ip="95\.11\.\d{1,3}\.111"
  -sf SQL_FILTER, --sql_filter SQL_FILTER
                        SQL like filter for more complex quering, e.g.
                        dnet="somednet" OR day-of-week>=2
  -o OUTPUT, --output OUTPUT
                        The full path and name of the file to save the result
                        to, defaults to current working dir/base_index
  -b, --banjax          Query Banjax logs instead of Deflect logs
```

To get data from deflect.log from "2018-04-23 10:00:00" to "2018-04-23 11:00:00"
where dnet equals somednet and client_ip equals 95.111.111.111
```
python simple_retrieve_cli.py "2018-04-23 10:00:00" "2018-04-23 11:00:00" --f dnet=somednet client_ip=95.111.111.111 -o /some/path/to/file
```

Sample output of the above:
```
18/04/24 15:47:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
18/04/24 15:47:38 WARN Utils: Your hostname, SOMEHOSTNAME resolves to a loopback address: 127.0.1.1; using SOMEOTHERIP instead (on interface SOMEINTERFACE)
18/04/24 15:47:38 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Getting data from 2018-04-23 10:00:00 to 2018-04-23 11:00:00, filter(s) Column<((dnet = somednet) AND (client_ip = 95.111.111.111))>
root
 |-- @timestamp: timestamp (nullable = true)
 |-- field1: string (nullable = true)
 |-- field2: string (nullable = true)
  ...
  ...
  ...

Starting data retrieval and saving to /some/path/to/file

```


### Loading data
The common steps are:
1. Import necessary
2. Create a configuration instance
3. Create a spark instance
4. Use `spark.read.json(path to folder)` or `spark.read.csv(path to folder)`
to load the data

```python
from es_retriever.config import Config
from es_retriever.spark import get_or_create_spark_session


config = Config(SERVER, 'user', 'password', 'deflect.log', 'deflect_access')
# create a spark session using the configuration
spark = get_or_create_spark_session(config)

# read all files that start with some-folder-name-previously-stored-with-spark
# - e.g. when you have different files for different days, this will load them
# all into one dataframe
# note: lazy loading and will spill to disc if ram not enough
df = spark.read.json('/path/to/some-folder-name-previously-stored-with-spark*')
```
#### Process after loading:
##### Cache the dataframe if not loading from file
Since spark operates lazily, we need to cache the dataframe once we get the data if we need to perform stuff on it.
E.g.
```python
df = storage.get(
    since,
    until,
    hours_filter
).filter(...)
.select(...)
.cache() # after filters

# then:
print df.count()
# if we do not cache the data will be fetched twice, once for load and filtering and once on count

```
In general, it is a good practice to *save the data first* and then, on a separate session, load from file and perform actions on the data.

##### Print the dataframe schema
```python
df.printSchema()
>>>
root
 |-- @timestamp: string (nullable = true)
 |-- field1: string (nullable = true)
 |-- field2: string (nullable = true)
 ...
```

##### How many logs are there?
```python
df.count()
>>>1276263
```

##### Get only two columns:
```python
df.select('@timestamp', 'field1').show()
>>>
+--------------------+--------------------+
|          @timestamp|              field1|
+--------------------+--------------------+
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:34:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:34:...|             data...|
|2018-04-01T10:36:...|             data...|
|2018-04-01T10:36:...|             data...|
+--------------------+--------------------+
only showing top 20 rows
```

##### Get the rows that contain a specific string
```python
specific_df = df.select("*").where(df.field1 == 'hello')

specific_df.show()  # will output the results

```

A more specific example:
```python
specific_df = df.select("*").where(
    (df.content_type == 'image/jpeg') & (df.dnet == 'somednet')
)

specific_df.show()  # will output the results

```

## Docker
To run the `simple_retrieve_cli.py`:
- Create a `.env` file according to the `dot_env_example`
- Modify `docker-compose.yaml` according to the options and filters explained in the [CLI example](#cli-example) section

```
command: python simple_retrieve_cli.py ${ELK_HOST} es-index es-index-type "start datetime" "end datetime" -u ${ELK_USER} -p -o /data/nameofthefolder
# for example:
command: python simple_retrieve_cli.py ${ELK_HOST} test.log web_access "2019-09-01 00:00:00" "2019-09-01 01:00:00" -u ${ELK_USER} -p -o /data/testdata
```
The example above will get all data between "2019-09-01 00:00:00" and "2019-09-01 01:00:00" and it will store them under `/data/testdata_YYYY_MM_dd` folder.
If the data range spans over more than a single day, then the data will be stored in separate folders `/data/testdata_YYYY_MM_day1`, `/data/testdata_YYYY_MM_day2` etc.

- Run: `docker-compose run --rm --service-ports es_retriever` - you will be prompted for your elastic search password. 
This process will probably take a lot of time to complete, depending on the date range and the criteria given, so it is better if this is run using e.g. `screen`.
You can also check the spark console under `localhost:4040`


## Limitations and Future Work
- Better docstrings
- Unittests to be added
- EsStorage assumes time based indices
- Config assumes ssl for es
- Currently, `geoip` and `tags` need to be filtered out otherwise save fails.
- Getting one years' data with 1h sampling (different hour each time)
needs some groupping for the requests to be efficient.
[note](https://stackoverflow.com/a/33537029/3433323)


<a rel="license" href="http://creativecommons.org/licenses/by/4.0/">
<img alt="Creative Commons Licence" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/80x15.png" /></a><br />
This work is copyright (c) 2020, eQualit.ie inc., and is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.
