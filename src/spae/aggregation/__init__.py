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
    def __init__(self, bucket, bucket_using, table=None, parent=None, left_id=None, right_id=None, using=None):
        '''
        [readbase] -- clientbaseid, id --> [clientbase]
        '''
        self.bucket = bucket
        self.bucket_using = bucket_using
        self.table = table
        self.parent = parent
        self.left_id = left_id
        self.right_id = right_id

    def transfer(self, target_table, left_id, right_id):
        return Entity(self.bucket, self.bucket_using, target_table, self, left_id, right_id)

    def reduce(self, aggregator, name):
        return Series(self, name, aggregator, self.bucket)

    def get_df(self):
        if self.parent:
            parent_df = self.parent.get_df()
            left_col = getattr(self.table.df, left_id)
            right_col = getattr(parent_df, right_id)
            return self.table.df.join(parent_df, left_col == right_col)
        else:
            return self.table.df

class Series:
    def __init__(self, entity, series_id, agg, bucket):
        self.id = series_id
        self.entity = entity
        self.agg = agg
        self.bucket = bucket

    def bucketize(self):
        df = self.entity.get_df()
        bucketizer = self.bucket.get_bucketizer(self.entity.bucket_using, f'bucket__{self.id}')
        result_df = self.bucket.bucketizer.transform(df)
        print(result_df.groupBy(f'bucket__{self.id}').agg(self.agg))
        return result_df


class Table:
    def __init__(self, spae, table_name):
        self.spae = spae
        self.table_name = table_name
        self.df = None
        self.columns = []

    def add_field(self, field):
        self.columns.append(field)

    def initialize(self):
        self.df = (
            self.spae.spark.read.format("jdbc")
            .option("url", f"jdbc:{ self.spae.db_url }") # postgresql://postgres:5432/postgres
            .option("driver", "org.postgresql.Driver") # currently only postgresql is supported.
            .option("dbtable", self.table_name)
            .option("user", self.spae.db_user)
            .option("password", self.spae.db_password)
            .load()
            .select(*self.columns)
        )


class Bucket:
    def __init__(self):
        self.name = None
        self.parent = None
        self.value_list = []
        self.max_value = None
        self.min_value = None
        self.step = timedelta(days=1)
        self.table = None
        self.using = 'datetime_stamp'

        self.bucketizer = None

    def get_bucketizer(self, from_col, to_col):
        return Bucketizer(splits=self.value_list, inputCol=from_col, outputCol=to_col)

    def initialize(self):
        # creating buckets
        df = self.table.df
        df = df.withColumn('datetime_stamp', unix_timestamp(df['datetime']))
        d_range = df.select(min(self.using), max(self.using)).collect()[0]
        min_value = d_range['min({self.using})'].replace(hour=0, minute=0, second=0, microsecond=0)
        max_value = d_range['max({self.using})']
        sc = spark.sparkContext
        step = self.step

        while min_value <= max_value:
            self.value_list.append(int(min_value.timestamp()))
            min_value += step


class Aggregation:
    def __init__(self, spae):
        self.buckets = {}
        self.tables = {}
        self.entities = {}
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
            table.initialize()

        for bucket_name, bucket in self.buckets.items():
            bucket.initialize()

        for series_name, series in self.series.items():
            series.bucketize()

    def get_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(self.spae, table_name)
        return self.tables[table_name]

    def create_enetity(self, table_name, bucket_name, field, name):
        table = self.get_table(table_name)
        table.add_field(field)
        self.entities[name] = Entity(self.buckets[bucket_name], field, table)

    def reduce_enetity(self, entity_name, aggregator, field, as_name):
        if aggregator == 'COUNT':
            agg = count(field)

        elif aggregator == 'LEN':
            agg = count(field)

        entity = self.entities[entity_name]

        if entity.table:
            entity.table.add_field(field)

        series = entity.reduce(agg, as_name)
        self.series[as_name] = series
