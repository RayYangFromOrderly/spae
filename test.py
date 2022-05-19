from src.space import Space
from src.space.aql.compiler import Compiler


Compiler().pre_compile(
    '''
    CREATE BUCKETS time_buckets TYPE DateTime
    '''
)
