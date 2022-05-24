class AggregationError(Exception):
    '''
    Error while running Aggregation
    '''


class BucketNameAlreadyExists(AggregationError):
    '''
    Try to name a bucket with existing bucket name
    '''


class BucketDoesNotExist(AggregationError):
    '''
    Try to name a bucket with existing bucket name
    '''


class DataSetEmpty(AggregationError):
    '''
    bucket is totally empty
    '''