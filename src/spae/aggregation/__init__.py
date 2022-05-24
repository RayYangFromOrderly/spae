from .exceptions import BucketDoesNotExist, BucketNameAlreadyExists

import pyspark
from pyspark.sql import SparkSession
from datetime import timedelta

from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import hour, mean
from pyspark.sql.functions import min, max, count

class DataType:
    def __init__(self):
        self.value_range = None

    def initialize(self, table):
        self.value_range = df.select(min('datetime'), max('datetime')).collect()[0]
        min_value = d_range['min(datetime)'].replace(hour=0, minute=0, second=0, microsecond=0)
        max_value = d_range['max(datetime)']


class Entity:
    def __init__(self, table=None, parent: Entity=None, left_id=None, right_id=None):
        '''
        [readbase] -- clientbaseid, id --> [clientbase]
        '''
        self.table = table
        self.parent = parent
        self.left_id = left_id
        self.right_id = right_id

    def transfer(self, target_table, left_id, right_id):
        return Entity(target_table, self, left_id, right_id)

    def reduce(self, aggregator):
        return Series(self, aggregator)

class Series:
    def __init__(self, entity, agg):
        self.entity = entity
        self.agg = agg


class Table:
    def __init__(self, table_name):
        self.table_name = table_name
        self.df = None
        self.columns = []

    def add_field(self, field):
        self.columns.append(field)

    def initialize(self):
        self.df = (
            spark.read.format("jdbc")
            .option("url", f"jdbc:{ self.spae.db_url }") # postgresql://postgres:5432/postgres
            .option("driver", "org.postgresql.Driver") # currently only postgresql is supported.
            .option("dbtable", table_name)
            .option("user", self.spae.db_user)
            .option("password", self.spae.db_password).load()
            .select(*self.columns)
        )
        return df

class Bucket:
    def __init__(self):
        self.name = None
        self.parent = None
        self.values_list = None
        self.max_value = None
        self.min_value = None
        self.step = timedelta(days=1)
        self.table = None
        self.using = 'datetime_stamp'

        self.bucketizer = None

    def initialize(self):
        # creating buckets
        df = table.df
        df = df.withColumn('datetime_stamp', unix_timestamp(df['datetime']))
        d_range = df.select(min(self.using), max(self.using)).collect()[0]
        min_value = d_range['min({self.using})'].replace(hour=0, minute=0, second=0, microsecond=0)
        max_value = d_range['max({self.using})']
        sc = spark.sparkContext
        step = self.step

        while min_value <= max_value:
            self.value_list.append(int(min_value.timestamp()))
            min_value += step

        self.bucketizer = Bucketizer(splits=self.value_list, inputCol=self.using, outputCol=self.name)


class Aggregation:
    def __init__(self, spae):
        self.buckets = {}
        self.tables = {}
        self.series = {}
        self.spae = spae
        self.collecting = []

    def create_buckets(self, bucket_name, type_name, continuous=False, parent=None):
        bucket = Bucket()

        if bucket_name in self.buckets:
            raise BucketDoesNotExist()
        else:
            self.buckets[bucket_name] = bucket

        if parent is not None:
            if parent not in self.buckets:
                raise BucketDoesNotExist()
            bucket.parent = parent

    def run(self):
        for table_name, table in self.tables.items():
            table.inisialize()

        for bucket_name, bucket in self.buckets.items():
            bucket.initialize()

        for table, series in self.series.items():
            series.bucketize()

        # transforming buckets
        from pyspark.sql.functions import unix_timestamp
        df = df.withColumn('datetime_stamp', unix_timestamp(df['datetime']))
        df_buck = bucketizer.setHandleInvalid("keep").transform(df)

        df_buck.groupBy('buckets').count()

    def get_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name)
        return self.tables[table_name]

    def create_enetity(table_name, bucket_name, field, name):
        table = self.get_table(table_name)
        table.add_field(field)
        self.entities[name] = Entity(table)

    def reduce_enetity(entity_name, aggregator, field, as_name):
        if aggregator == 'COUNT':
            agg = count(field)

        elif aggregator == 'LEN':
            agg = count(field)

        series = self.entities[entity_name].reduce(agg)
        self.series[as_name] = series
