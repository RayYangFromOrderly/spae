import pyspark
from pyspark.sql import SparkSession
from datetime import timedelta

from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import *

from .utils import get_column
from .data import types, DataType, DateTime, Category
from .exceptions import BucketDoesNotExist, BucketNameAlreadyExists, DataSetEmpty


class Column:
    def __init__(self, table, column):
        self.annotated = False
        self.table = table
        self.original_col = column
        self.handler = None
        self.column = column

    def initialize(self):
        if self.column in self.table.df.schema:
            type_cls = self.table.df.schema[self.column].dataType.__class__
            self.handler = types.get(type_cls, DataType)
            self.column = self.handler.preprocess_column(self.table, self.column)

    def __eq__(self, obj):
        return self.column == obj.column


class Entity:
    def __init__(self, bucket, bucket_using, table=None, parent=None, left_id=None, right_id=None, using=None, has_condition=False, condition=None):
        '''
        [readbase] -- clientbaseid, id --> [clientbase]
        '''
        self.bucket = bucket
        self.bucket_using = bucket_using
        self.table = table
        self.parent = parent
        self.left_id = left_id
        self.right_id = right_id
        self.has_condition = has_condition
        self.condition = condition

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
            df = self.table.df.join(parent_df, left_col == right_col)
        else:
            df = self.table.df

        if self.bucket_using:
            df = df.where(col(self.bucket_using.column).isNotNull())

        if self.has_condition:
            df = df.filter(self.condition)
        return df


class Series:
    def __init__(self, entity, series_id, agg, bucket):
        self.id = series_id
        self.entity = entity
        self.agg = agg
        self.bucket = bucket

    def get_buckets(self):
        return self.bucket.get_buckets()

    def bucketize(self):
        if self.bucket.empty:
            return None

        df = self.entity.get_df()
        column = self.entity.bucket_using
        if self.bucket.handler != Category:
            bucketizer = self.bucket.get_bucketizer(column.column)
            df = bucketizer.transform(df)
        else:
            df = df.withColumn(self.bucket.get_column_name(), df[column.column])
        df = df.groupBy(self.bucket.get_column_name()).agg(self.agg.alias(self.id))
        return df


class Table:
    def __init__(self, spae, table_name):
        self.spae = spae
        self.table_name = table_name
        self.df = None
        self.columns = {}
        self.annotations = []

    def add_fields(self, *fields, annotated=False):
        for field in fields:
            if field not in self.columns:
                column = Column(self, field)
                self.columns[field] = column
            else:
                column = self.columns[field]
            if annotated:
                column.annotated = True

        return column

    def annotate(self, annotation, as_field):
        self.annotations.append((annotation, as_field))

    def initialize(self):
        self.df = (
            self.spae.spark.read.format("jdbc")
            .option("url", f"jdbc:{ self.spae.db_url }") # postgresql://postgres:5432/postgres
            .option("driver", "org.postgresql.Driver") # currently only postgresql is supported.
            .option("dbtable", self.table_name)
            .option("user", self.spae.db_user)
            .option("password", self.spae.db_password)
            .load()
            .select(*[column.column for column in self.columns.values() if not column.annotated])
        )
        for annotation, as_field in self.annotations:
            self.df = self.df.withColumn(as_field, eval(annotation))

        for column in self.columns.values():
            column.initialize()


class Bucket:
    def __init__(self, spae=None, name=None, handler=None):
        self.spae = spae
        self.name = name
        self.parent = None
        self.series_list = []
        self.value_list = []
        self.children = []
        self.max_value = None
        self.min_value = None
        self.handler = handler
        self.step = 1
        self.empty = False
        self.tables = [
            # (table, using_field)
        ]
        self.df = None
        self.bucketizer = None

    def add_child(self, bucket):
        for child in self.children:
            if bucket.name == child.name:
                return

        self.children.append(bucket)

    def fill(self, container):
        '''
        fill value recursively, current container will be like:
        {
            'slots': [
                {
                    value: 2,
                    'other_bucket': {}
                }
            ]
        }
        '''
        for value in self.value_list:
            if value and self.handler:
                value = self.handler.get_value(value)
            slot = {
                'value': value,
            }
            container['slots'].append(slot)
            for series in self.series_list:
                slot[series.id] = 0

            for child in self.children:
                child_container = {'slots': []}
                slot[child.name] = child_container
                child.fill(child_container)

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
        if self.min_value is not None:
            self.min_value = self.handler.get_min_value(self.min_value)

        if self.max_value is not None:
            self.max_value = self.handler.get_max_value(self.max_value)
        min_value = max_value = None
        # creating buckets
        for table, using, condition in self.tables:
            df = table.df
            _min, _max = self.handler.get_range(table, using, condition)
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
        try:
            self.value_list = self.handler.get_value_list(min_value, max_value, self.step)
        except DataSetEmpty:
            self.empty = True

    def add_table(self, table, using, condition):
        self.tables.append((table, using, condition))

class Aggregation:
    def __init__(self, spae):
        self.buckets = {}
        self.tables = {}
        self.entities = {}
        self.series = {}
        self.returning_series = {}
        self.spae = spae
        self.collecting = []

    def create_buckets(self, bucket_name, type_name, continuous=False, parent=None):
        bucket = Bucket(self.spae, bucket_name, types.get(type_name, DataType))

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

        leaf_buckets = []
        bucket_data = {'slots': []}
        root_bucket = Bucket()
        root_bucket.value_list = [None]

        for series in self.returning_series.values():
            leaf_buckets.append(series.bucket)

        for leaf_bucket in leaf_buckets:
            buckets = [root_bucket] + leaf_bucket.get_buckets()
            for i, bucket in enumerate(buckets[:-1]):
                bucket.add_child(buckets[i+1])

        root_bucket.fill(bucket_data)
        bucket_data = bucket_data['slots'][0]

        for series_name, series in self.returning_series.items():
            leaf_buckets.append(series.bucket)
            series_df = series.bucketize()
            if not series_df:
                continue

            for item in series_df.collect():
                buckets = series.get_buckets()
                bucket_dict = bucket_data
                for bucket in buckets:
                    val = int(item[bucket.get_column_name()])
                    bucket_dict = bucket_dict[bucket.name]['slots'][val]
                bucket_dict[series_name] = item[series_name]

        return bucket_data

    def get_table(self, table_name):
        if table_name not in self.tables:
            self.tables[table_name] = Table(self.spae, table_name)
        return self.tables[table_name]

    def create_entity(self, table_name, bucket_name, field, name, has_condition, condition):
        table = self.get_table(table_name)
        column = table.add_fields(field)
        bucket = self.buckets[bucket_name]
        bucket.add_table(table, column, condition)
        entity = Entity(bucket, column, table, has_condition=has_condition, condition=condition)
        self.entities[name] = entity
        return entity

    def return_series(self, serires_names):
        for series_name in serires_names:
            self.returning_series[series_name] = self.series.get(series_name)

    def reduce_enetity(self, entity_name, aggregator, field, as_name):

        aggregator = aggregator(field)

        entity = self.entities[entity_name]

        if entity.table:
            column = entity.table.add_fields(field)

        series = entity.reduce(aggregator, as_name)
        entity.bucket.series_list.append(series)
        self.series[as_name] = series
