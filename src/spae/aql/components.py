from pyspark.sql.functions import min, max, count, unix_timestamp, hour, mean, sum, avg

from .exceptions import AqlError, CommandSyntaxError

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
            except CommandSyntaxError as e:
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
    def resolve(self, source_components, index):
        if not index < len(source_components):
            raise CommandSyntaxError('a name')
        else:
            aggregator_map = {
                'COUNT': count,
                'MAX': max,
                'MIN': min,
                'COUNT': count,
                'SUM': sum,
                'AVG': avg,
                'MEAN': mean
            }
            try:
                aggregator = aggregator_map[source_components[index]]
            except KeyError:
                raise CommandSyntaxError(f'one of: {list(aggregator_map.keys())}', source=source_components[index])

            return index+1, [aggregator]


class Bracketed:
    def resolve(self, source_components, index):
        string = ''
        left_brack_count = 1
        if not index < len(source_components):
            raise CommandSyntaxError(f'(', arg_count=1)
        elif source_components[index] != '(':
            raise CommandSyntaxError(f'(', source=source_components[index], arg_count=1)
        index += 1
        while index < len(source_components):
            if source_components[index] == ')':
                left_brack_count -= 1
            elif source_components[index] == '(':
                left_brack_count += 1

            if left_brack_count == 0:
                return index + 1, [string]

            string += ' ' + source_components[index]
            index += 1

        raise CommandSyntaxError(f')', arg_count=1)


class OneOrMore(Arg):
    def __init__(self, *pattern, flat=False):
        self.pattern = pattern
        self.flat = flat

    def resolve(self, source_components, index):
        all_args = []
        while True:
            for component in self.pattern:
                index, args = component.resolve(source_components, index)
                if self.flat and args:
                    all_args.append(args[0])
                else:
                    all_args.append(args)

            if index < len(source_components):
                if source_components[index] == ',':
                    index += 1
                    continue
            break

        return index, [all_args]


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

    def run(self, aggregation):
        pass


class DEFINE(Command):
    '''
    DEFINE age (now() - bday)
    '''
    pattern = [
        Arg(),
        Bracketed(),
    ]
    def simulate(self, aggregation):
        pass

    def run(self, aggregation):
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
            Text('MIN'),
            Arg()
        ),
        Optional(
            Text('MAX'),
            Arg()
        ),
        Optional(
            Text('STEP'),
            Arg()
        ),
        Optional(
            Text('CONTINUOUS'),
        ),
        Optional(
            Text('IN'),
            BucketName()
        )
    ]
    def __init__(
        self, bucket_name, type_name,
        custom_min, min_value,
        custom_max, max_value,
        custom_step, step_value,
        continuous, is_subbucket, parent_bucket_name
    ):
        super().__init__()
        self.type_name = type_name
        self.min_value = min_value
        self.max_value = max_value
        self.step_value = step_value
        self.continuous = continuous
        self.bucket_name = bucket_name
        self.is_subbucket = is_subbucket
        self.parent_bucket_name = parent_bucket_name

    def simulate(self, aggregation):
        if self.is_subbucket:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous, parent=self.parent_bucket_name)
        else:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous)

    def run(self, aggregation):
        if self.is_subbucket:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous, parent=self.parent_bucket_name)
        else:
            aggregation.create_buckets(self.bucket_name, self.type_name, continuous=self.continuous)

        aggregation.min_value = self.min_value
        aggregation.max_value = self.max_value
        aggregation.step = self.step_value


class LET(Command):
    '''
    SELECT CLIENTBASE FALLS INTO time_buckets USING join_datetime as clientbases ANNOTATING () AS age
    '''
    pattern = [
        TableName(),
        Text('FALLS'),
        Text('INTO'),
        BucketName(),
        Text('USING'),
        Arg(),
        Text('AS'),
        Arg(),
        Optional(
            Text('WHERE'),
            Bracketed()
        ),
        Optional(
            Text('ANNOTATING'),
            Bracketed(),
            Text('AS'),
            Arg()
        ),
        Optional(
            Text('FIELDS'),
            OneOrMore(Arg(), flat=True)
        ),
    ]
    def __init__(self, table_name, bucket_name, field, name, has_condition, condition, has_annotation, annotation, as_field, has_fields, additional_fields):
        super().__init__()
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.field = field
        self.name = name
        self.has_condition = has_condition
        self.condition = condition
        self.has_annotation = has_annotation
        self.annotation = annotation
        self.as_field = as_field
        self.has_fields = has_fields
        self.additional_fields = additional_fields

    def run(self, aggregation):
        entity = aggregation.create_entity(self.table_name, self.bucket_name, self.field, self.name, self.has_condition, self.condition,)
        if self.has_fields:
            entity.table.add_fields(*self.additional_fields)

        if self.has_annotation:
            entity.table.annotate(self.annotation, self.as_field)
            entity.table.add_fields(self.as_field, annotated=True)


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
    def __init__(self, entity_name, aggregator, field, as_name):
        super().__init__()
        self.entity_name = entity_name
        self.aggregator = aggregator
        self.field = field
        self.as_name = as_name

    def run(self, aggregation):
        aggregation.reduce_enetity(self.entity_name, self.aggregator, self.field, self.as_name)


class RETURN(Command):
    '''
    RETURN clientbases IN time_series
    '''
    pattern = [
        OneOrMore(SeriesName(), flat=True),
        Text('IN'),
        BucketName()
    ]
    def __init__(self, series_names, bucket_name):
        super().__init__()
        self.series_names = series_names
        self.bucket_name = bucket_name

    def run(self, aggregation):
        aggregation.return_series(self.series_names)

class TRANSFER(Command):
    '''
    TRANSFER clientbases TO ReadBase WHERE clientbases.id=OrderBase.clientbase_id
    '''
    pattern = [
        SeriesName(),
        Text('IN'),
        BucketName()
    ]
    def __init__(self, series_name, bucket_name):
        super().__init__()
        self.series_name = series_name
        self.bucket_name = bucket_name
