from .exceptions import BucketDoesNotExist, BucketNameAlreadyExists

class Bucket:
    def __init__(self):
        self.parent = None


class Aggregation:
    def __init__(self):
        self.buckets = {}

    def create_buckets(self, bucket_name, type_name, continuous=False, parent=None):
        bucket = Bucket()

        if bucket_name in self.buckets:
            raise BucketDoesNotExist()
        else:
            self.buckets[bucket_name] = bucket

        if parent is not None:
            if parent not in self.buckets:
                raise BucketDoesNotExist()
            bucket.parent = parent
