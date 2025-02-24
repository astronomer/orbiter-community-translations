from ast2json import str2json
from orbiter.file_types import FileType

def dump_python(val: dict) -> str:
    raise NotImplementedError("Python dumping is not implemented yet.")

class FileTypePython(FileType):
    extension = {"py"}

    load_fn = str2json
    dump_fn = dump_python
