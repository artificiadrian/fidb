import os
from typing import Optional
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from argh import ArghParser, arg
from db import Path, PathType, to_path
import sys
from rich.console import Console

LOADING = "[*]"
SUCCESS = "[green][+][/green]"
ERROR = "[red][-][/red]"

console = Console()


def init_engine(db):
    engine = create_engine(db)
    Path.metadata.create_all(engine)
    return engine


def add_paths_from_generator(generator, engine):
    count = 0

    with Session(engine) as session:
        for path in generator:
            count += 1
            session.add(to_path(path))
            yield count
            if count % 100_000 == 0:
                session.commit()
        session.commit()
    console.print(f"{SUCCESS} Added {count} paths")


@arg("file", help="Zip file containing path list file (first file in zip, one path per line)")
def unzip(file: str, **kwargs):
    """Read paths from zip"""
    import zipfile
    import io

    with zipfile.ZipFile(file) as f:
        msg = "Adding paths from zip (%d so far)..."
        with console.status(msg % 0) as status:
            for i in add_paths_from_generator(io.TextIOWrapper(f.open(f.filelist[0])),
                                              init_engine(kwargs["db"])):
                if i % 10_000 == 0:
                    status.update(msg % i)


def read(**kwargs):
    """Read paths from stdin"""
    console.print(f"{LOADING} Adding paths from stdin...")

    def generator():
        for line in sys.stdin:
            if line.strip() == "":
                break
            yield line.strip()

    for _ in add_paths_from_generator(generator(), init_engine(kwargs["db"])):
        pass


@arg("path_type", choices=[PathType.linux.value, PathType.windows.value], help="Path type (linux or windows)")
@arg("-r", "--regex", help="Only return paths matching regex")
@arg("-m", "--min-weight", default=1, type=int, help="Minimum weight of path")
@arg("-s", "--search", help="Only return paths containing string")
@arg("-t", "--transform", help="Transform path before printing (use {path}, {name} and {dir} as placeholders)")
@arg("-o", "--only", choices=["dirs", "files"], help="Only return directories or files")
def query(path_type: PathType, ** kwargs):
    """Query paths"""
    engine = init_engine(kwargs["db"])
    print(kwargs)
    with Session(engine) as session:
        query = session.query(Path.value, func.count(Path.value).label("weight"))\
            .group_by(Path.value)\
            .where(Path.type == path_type)

        if kwargs["only"] is not None:
            query = query\
                .where(Path.is_dir == (kwargs["only"] == "dirs"))

        if kwargs["regex"] is not None:
            query = query\
                .having(Path.value.regexp_match(kwargs["regex"]))

        if kwargs["search"] is not None:
            query = query\
                .having(Path.value.like(f"%{kwargs['search']}%"))

        query = query\
            .having(text("weight>=:min_weight").bindparams(min_weight=kwargs["min_weight"]))\
            .order_by(text("value asc, weight desc"))

        def transformer(path):
            if kwargs["transform"] is not None:
                return kwargs["transform"]\
                    .replace("{path}", path)\
                    .replace("{name}", os.path.basename(path))\
                    .replace("{dir}", os.path.dirname(path))

            return path

        count = 0
        for path, _ in query:
            print(transformer(path))
            count += 1

        if count == 0:
            console.print(f"{ERROR} No paths found for given query")


parser = ArghParser()
parser.add_argument("--db", default="sqlite:///db.sqlite",
                    help="Database connection string")
parser.add_commands([unzip, read, query])

if __name__ == "__main__":
    parser.dispatch()
