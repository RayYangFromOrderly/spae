class AqlError(Exception):
    pass


class ComponentError(AqlError):
    def __init__(self, arg_count=0):
        self.arg_count = 0


class CommandNotFoundError(ComponentError):
    def __init__(self, cmd, cmd_pool, **kwargs):
        super().__init__(f'Command {cmd} not found, shoud be one of {cmd_pool}', **kwargs)


class CommandSyntaxError(ComponentError):
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

