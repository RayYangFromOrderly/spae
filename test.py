from src.spae import spae
from src.spae.aql.compiler import Compiler


Compiler().pre_compile(
    '''
    CREATE BUCKETS time_buckets TYPE DateTime
    LET clientbase FALLS INTO time_buckets USING clientbase.join_datetime AS clientbases
    '''
)
