from aql.compiler import Compiler


class Spae:
    '''
    spae Client for AQL Compilation
    '''

    def __init__(self, spark_url, db_url):
        self.spark_url = spark_url
        self.db_url = db_url
        self.spark = SparkSession.builder.master(self.spark_url).config("spark.jars", "/postgresql-42.3.5.jar").appName('abc').getOrCreate()

    def aggregate(self, aql):
        compiler = Compiler()
        compiler.pre_compile(aql)
        compiler.run()