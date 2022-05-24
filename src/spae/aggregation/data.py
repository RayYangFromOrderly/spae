import datetime

from pyspark.sql.types import TimestampType
from pyspark.sql.functions import unix_timestamp, min, max

types = {}
def handles(data_type):

    def registry(data_type_definition):
        types[data_type] = data_type_definition
        return data_type_definition

    return registry

class DataType:
    @staticmethod
    def get_range(table, column):
        using = column.column
        d_range = table.df.select(min(using).alias('__min'), max(using).alias('__max')).collect()[0]
        return d_range[f'__min'], d_range[f'__max']

    @staticmethod
    def preprocess_column(table, column):
        return column

    @staticmethod
    def get_value(value):
        return value

    @staticmethod
    def get_step(step):
        return step

    @classmethod
    def get_value_list(cls, min_value, max_value, step):
        step = cls.get_step(step)
        value_list = []
        while min_value < max_value:
            value_list.append(min_value)
            min_value += step

        return [float('-Inf')] + value_list + [float('Inf')]


@handles(TimestampType)
class DateTime(DataType):
    @staticmethod
    def preprocess_column(table, column):
        trans_to = f'spae__{column}__stamp'
        table.df = table.df.withColumn(trans_to, unix_timestamp(table.df[column]))
        return trans_to

    @staticmethod
    def get_value(value):
        return datetime.datetime.fromtimestamp(value)

    @staticmethod
    def get_step(step):
        return datetime.timedelta(days=1).total_seconds()
