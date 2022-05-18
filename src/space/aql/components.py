class Pattern:
    pass

class Component:
    pass


class Optional:
    pass


class Text(Component):
    pass


class Arg(Component):
    pass


class TypeName(Arg):
    pass


class BucketName(Arg):
    pass


class TableName(Arg):
    pass


class KeyWord:
    pass


class CREATE(KeyWord):
    '''
    CREATE BUCKETS time_buckets TYPE DateTime CONTINUOUS
    '''
    pattern = [
        Text('BUCKETS'),
        Arg(),
        Text('TYPE'),
        TypeName(),
        Optional(
            Text('CONTINUOUS'),
        ),
        Optional(
            Text('IN'),
            BucketName()
        )
    ]
    def resolve(self, bucket_name, type_name, continuous, is_subbucket, parent_bucket_name):
        self.bucket_name = bucket_name
        self.type_name = type_name
        self.continuous = continuous
        self.is_subbucket = is_subbucket
        self.parent_bucket_name = parent_bucket_name


class SELECT(KeyWord):
    '''
    SELECT CLIENTBASE FALLS INTO time_buckets USING join_datetime as clientbases
    '''
    pattern = [
        TableName(),
        Text('FALLS'),
        Text('INTO'),
        BucketName(),
        Text('USING'),
        Arg(),
        Text('AS'),
        Arg()
    ]
    def resolve(self, table_name, bucket_name, field, name):
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.field = field
        self.name = name
