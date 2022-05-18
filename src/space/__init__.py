class Space:
    '''
    Space Client for AQL Compilation
    '''

    def __init__(self, spark_url, db_url):
        self.spark_url = spark_url
        self.db_url = db_url

