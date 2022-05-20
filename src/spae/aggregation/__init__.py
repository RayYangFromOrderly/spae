class Bucket:
    pass

class Aggregation:
    def __init__(self):
        self.buckets = {}

    def create_buckets(self, bucket_name, parent_bucket):
        if bucket_name in self.buckets:
            raise Exception()
