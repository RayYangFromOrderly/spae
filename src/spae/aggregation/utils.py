from pyspark.sql.types import TimestampType
from pyspark.sql.functions import unix_timestamp

def get_column(table, column):
    df = table.df

    if isinstance(df.schema[column].dataType,TimestampType):
        as_column = f'{column}__stamp'
        if as_column not in df.schema:
            table.df = df.withColumn(as_column, unix_timestamp(df[column]))
        return as_column
    return column
