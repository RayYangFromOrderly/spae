from .exceptions import BucketDoesNotExist, BucketNameAlreadyExists

import pyspark
from pyspark.sql import SparkSession
from datetime import timedelta

from pyspark.ml.feature import Bucketizer

from .utils import get_column
from .data import types, DataType, DateTime

class Column:
    def __init__(self, table, column):
        self.table = table
        self.original_col = column
        self.handler = None
        self.column = column

    def initialize(self):
        type_cls = self.table.df.schema[self.column].dataType.__class__
        self.handler = types.get(type_cls, DataType)
        self.column = self.handler.preprocess_column(self.table, self.column)

    def __eq__(self, obj):
        return self.column == obj.column


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
        column = Column(target_table, self.bucket_using)
        return Entity(self.bucket, column, target_table, self, left_id, right_id)

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

    def get_buckets(self):
        return self.bucket.get_buckets()

    def bucketize(self):
        df = self.entity.get_df()
        column = self.entity.bucket_using
        bucketizer = self.bucket.get_bucketizer(column.column)
        result_df = bucketizer.transform(df)
        result_df = result_df.groupBy(self.bucket.get_column_name()).agg(self.agg.alias(self.id))
        return result_df


class Table:
    def __init__(self, spae, table_name):
        self.spae = spae
        self.table_name = table_name
        self.df = None
        self.columns = {}

    def add_field(self, field):
        column = Column(self, field)
        if field not in self.columns:
            self.columns[field] = column
        return column

    def initialize(self):
        self.df = (
            self.spae.spark.read.format("jdbc")
            .option("url", f"jdbc:{ self.spae.db_url }") # postgresql://postgres:5432/postgres
            .option("driver", "org.postgresql.Driver") # currently only postgresql is supported.
            .option("dbtable", self.table_name)
            .option("user", self.spae.db_user)
            .option("password", self.spae.db_password)
            .load()
            .select(*[column.column for column in self.columns.values()])
        )
        for column in self.columns.values():
            column.initialize()


class Bucket:
    def __init__(self, spae, name, handler):
        self.spae = spae
        self.name = name
        self.parent = None
        self.value_list = []
        self.max_value = None
        self.min_value = None
        self.handler = handler
        self.step = timedelta(days=1)
        self.tables = [
            # (table, using_field)
        ]
        self.df = None
        self.bucketizer = None

    def get_value(self, value):
        return self.handler.get_value(self.value_list[int(value)])

    def get_column_name(self):
        return f'bucket__{self.name}'

    def get_bucketizer(self, column):
        return Bucketizer(splits=self.value_list, inputCol=column, outputCol=self.get_column_name())

    def get_buckets(self):
        parents = []
        if self.parent:
            parents = self.parent.get_buckets()
        return parents + [self]

    def initialize(self):
        min_value = None
        max_value = None
        # creating buckets
        for table, using in self.tables:
            df = table.df
            _min, _max = self.handler.get_range(table, using)
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

        self.value_list = self.handler.get_value_list(min_value, max_value, self.step)

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
        bucket = Bucket(self.spae, bucket_name, DateTime)

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
                    if bucket.name not in bucket_dict:
                        bucket_dict[bucket.name] = {}
                    bucket_dict = bucket_dict[bucket.name]
                    bucket_key = bucket.get_value(item[bucket.get_column_name()])
                    next_bucket = bucket_dict[bucket_key] = bucket_dict.get(bucket_key, {})
                    bucket_dict = next_bucket
                bucket_dict[series_name] = item[series_name]
        return result

    def get_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(self.spae, table_name)
        return self.tables[table_name]

    def create_enetity(self, table_name, bucket_name, field, name):
        table = self.get_table(table_name)
        column = table.add_field(field)
        bucket = self.buckets[bucket_name]
        bucket.add_table(table, column)
        self.entities[name] = Entity(bucket, column, table)

    def reduce_enetity(self, entity_name, aggregator, field, as_name):

        entity = self.entities[entity_name]

        if entity.table:
            column = entity.table.add_field(field)

        series = entity.reduce(aggregator, as_name)
        self.series[as_name] = series
