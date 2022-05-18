class AqlError(Exception):
    pass


class CommandNotFoundError(Exception):
    def __init__(self, cmd, cmd_pool):
        super().__init__(f'Command {cmd} not found, shoud be one of {cmd_pool}')


class PreCompileError(Exception):
    pass


class SchemaError(PreCompileError):
    pass

