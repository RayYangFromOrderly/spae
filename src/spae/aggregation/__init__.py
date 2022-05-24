from .exceptions import BucketDoesNotExist, BucketNameAlreadyExists

import pyspark
from pyspark.sql import SparkSession
from datetime import timedelta

from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import min, max, count, unix_timestamp, hour, mean

from .utils import get_column

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
        column = get_column(self.entity.table, self.entity.bucket_using)
        bucketizer = self.bucket.get_bucketizer(column)
        result_df = bucketizer.transform(df)

        return result_df.groupBy(self.bucket.get_column_name()).agg(self.agg.alias(self.id))


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
        for column in self.columns:
            get_column(self, column)


class Bucket:
    def __init__(self, spae, name):
        self.spae = spae
        self.name = name
        self.parent = None
        self.value_list = []
        self.max_value = None
        self.min_value = None
        self.step = timedelta(days=1)
        self.tables = [
            # (table, using_field)
        ]
        self.using = 'datetime_stamp'
        self.df = None
        self.bucketizer = None

    def get_column_name(self):
        return f'bucket__{self.name}'

    def get_bucketizer(self, column):
        return Bucketizer(splits=self.value_list, inputCol=column, outputCol=self.get_column_name())

    def initialize(self):
        min_value = None
        max_value = None
        # creating buckets
        for table, using in self.tables:
            df = table.df
            if using == 'datetime':
                df = df.withColumn('datetime_stamp', unix_timestamp(df['datetime']))
            d_range = df.select(min(using), max(using)).collect()[0]
            _min = d_range[f'min({using})'].replace(hour=0, minute=0, second=0, microsecond=0)
            _max = d_range[f'max({using})']
            if min_value:
                if _min < min_value:
                    min_value = _min
            else:
                min_value = _min

            if max_value:
                if _max > max_value:
                    max_value = _max
            else:
                max_value = _max
        step = self.step

        while min_value <= max_value:
            self.value_list.append(int(min_value.timestamp()))
            min_value += step

    def add_table(self, table, using):
        self.tables.append((table, using))

class Aggregation:
    def __init__(self, spae):
        self.buckets = {}
        self.tables = {}
        self.entities = {}
        self.series = {}
        self.spae = spae
        self.collecting = []

    def create_buckets(self, bucket_name, type_name, continuous=False, parent=None):
        bucket = Bucket(self.spae, bucket_name)

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

        result = {}

        for series_name, series in self.series.items():
            series_df = series.bucketize()
            for item in series_df.collect():
                buckets = series.get_buckets()
                bucket_dict = result
                for bucket in buckets:
                    bucket_key = item[buckets]
                    bucket_dict = bucket_dict[bucket_key] = bucket_dict.get(bucket, {})
                bucket_dict[series_name] = item[series_name]
        return result

    def get_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(self.spae, table_name)
        return self.tables[table_name]

    def create_enetity(self, table_name, bucket_name, field, name):
        table = self.get_table(table_name)
        table.add_field(field)
        bucket = self.buckets[bucket_name]
        bucket.add_table(table, field)
        self.entities[name] = Entity(bucket, field, table)

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
