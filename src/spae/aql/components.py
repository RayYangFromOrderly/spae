from .exceptions import AqlError, CommandSyntaxError, ComponentError

components = {}
commands = {}

class ComponentRegistry(type):
    def __init__(cls, cls_name, bases, attrs):
        super().__init__(cls_name, bases, attrs)
        if bases:
            commands[cls_name] = cls


class Pattern:
    pass


class Component:
    def resolve(self, source_components, index):
        return index, []


class Optional:
    def __init__(self, *pattern):
        self.pattern = pattern

    def resolve(self, source_components, index):
        all_args = []
        _i = index
        passed = True
        for component in self.pattern:
            try:
                _i, args = component.resolve(source_components, _i)
                all_args += args
            except ComponentError as e:
                passed = False
                arg_count = e.arg_count
                all_args += [None] * arg_count
        if passed:
            index = _i
        else:
            all_args = [None for _ in all_args]

        return index, [passed] + all_args


class Text(Component):
    def __init__(self, text):
        self.text = text

    def resolve(self, source_components, index):
        if not index < len(source_components):
            raise CommandSyntaxError(self.text)

        source_text = source_components[index]
        if source_text.upper() == self.text.upper():
            return index+1, []
        else:
            raise CommandSyntaxError(self.text, source=source_text)


class Arg(Component):
    def resolve(self, source_components, index):
        if not index < len(source_components):
            raise CommandSyntaxError('a name')
        else:
            return index+1, [source_components[index]]


class Aggregator(Arg):
    pass


class TypeName(Arg):
    pass


class BucketName(Arg):
    pass


class TableName(Arg):
    pass


class SeriesName(Arg):
    pass


class Command(metaclass=ComponentRegistry):
    pattern = []
    def simulate(self, aggregation):
        pass


class CREATE(Command):
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
    def __init__(self, bucket_name, type_name, continuous, is_subbucket, parent_bucket_name):
        super().__init__()
        self.bucket_name = bucket_name
        self.type_name = type_name
        self.continuous = continuous
        self.is_subbucket = is_subbucket
        self.parent_bucket_name = parent_bucket_name

    def simulate(self, aggregation):
        if self.is_subbucket:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous, parent=self.parent_bucket_name)
        else:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous)


class LET(Command):
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
    def __init__(self, table_name, bucket_name, field, name):
        super().__init__()
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.field = field
        self.name = name


class REDUCE(Command):
    '''
    SELECT CLIENTBASE FALLS INTO time_buckets USING join_datetime as clientbases
    '''
    pattern = [
        SeriesName(),
        Text('AGG'),
        Aggregator(),
        Arg(),
        Text('AS'),
        Arg()
    ]
    def __init__(self, series_name, aggregator, field, as_name):
        super().__init__()
        self.series_name = series_name
        self.aggregator = aggregator
        self.field = field
        self.as_name = as_name


class RETURN(Command):
    '''
    SELECT CLIENTBASE FALLS INTO time_buckets USING join_datetime as clientbases
    '''
    pattern = [
        SeriesName(),
        Text('ON'),
        BucketName()
    ]
    def __init__(self, series_name, bucket_name):
        super().__init__()
        self.series_name = series_name
        self.bucket_name = bucket_name