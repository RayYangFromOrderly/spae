from uuid import uuid4
from .aql.compiler import Compiler
from pyspark.sql import SparkSession


class Spae:
    '''
    spae Client for AQL Compilation
    '''

    def __init__(self, spark_url, db_url, db_user, db_password, additional_jars=[], lazy=False):
        self.spark_url = spark_url
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.lazy = lazy
        self.spark = None
        self.additional_jars = additional_jars
        if not lazy:
            self.build_session()

    def build_session(self):
        builder = SparkSession.builder.master(self.spark_url)
        for jar in self.additional_jars:
            builder = builder.config("spark.jars", jar)

        self.spark = (
            builder
            .appName('SPAE')
            .getOrCreate()
        )

    def aggregate(self, aql):
        if self.spark is None:
            self.build_session()

        compiler = Compiler(self)
        compiler.pre_compile(aql)
        return compiler.run()