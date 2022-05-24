from src.spae import Spae
from src.spae.aql.compiler import Compiler


spae = Spae('spark://spark:7077', '')

Compiler().pre_compile(
    '''
    CREATE BUCKETS time_buckets TYPE DateTime MIN TruncateDate(auto) MAX auto STEP 1d CONTINUOUS
    LET clientbase FALLS INTO time_buckets USING clientbase.join_datetime AS clientbases
    REDUCE clientbase AGG COUNT_UNIQUE clientbase.id AS client_counts
    RETURN client_counts IN time_buckets
    '''
)
