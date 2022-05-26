class AqlError(Exception):
    pass


class AQLSyntaxError(AqlError):
    def __init__(self, msg, arg_count=0):
        super().__init__(msg)
        self.arg_count = arg_count


class CommandNotFoundError(AQLSyntaxError):
    def __init__(self, cmd, cmd_pool, **kwargs):
        super().__init__(f'Command {cmd} not found, shoud be one of {cmd_pool}', **kwargs)


class CommandSyntaxError(AQLSyntaxError):
    def __init__(self, text, source=None, **kwargs):
        if source:
            super().__init__(f'{source} cannot be resolved, expecting {text}', **kwargs)
        else:
            super().__init__(f'command ends unexpectingly, expecting {text}', **kwargs)
        self.text = text
        self.source = source


class PreCompileError(Exception):
    pass


class SchemaError(PreCompileError):
    pass
