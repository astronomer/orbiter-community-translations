from orbiter.orbiter.file_types import FileType


class FileTypeWLD(FileType):
    extension = {"WLD"}
    load_fn = print
    dump_fn = print
