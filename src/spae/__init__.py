from .aql.compiler import Compiler
from pyspark.sql import SparkSession


class Spae:
    '''
    spae Client for AQL Compilation
    '''

    def __init__(self, spark_url, db_url, db_user, db_password):
        self.spark_url = spark_url
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.spark = SparkSession.builder.master(self.spark_url).config("spark.jars", "/postgresql-42.3.5.jar").appName('abc').getOrCreate()

    def aggregate(self, aql):
        compiler = Compiler(self)
        compiler.pre_compile(aql)
        return compiler.run()