#!/usr/bin/env python3

import os
from typing import Optional
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from argh import ArghParser, arg
from db import FiPath, PathType, to_path
import sys
from rich.console import Console
from pathlib import PurePosixPath, PureWindowsPath, PurePath

LOADING = "[*]"
SUCCESS = "[green][+][/green]"
ERROR = "[red][-][/red]"

console = Console()


def init_engine(db):
    engine = create_engine(db)
    FiPath.metadata.create_all(engine)
    return engine


def value_to_path(value: str, type: PathType) -> PurePath:
    if type == PathType.linux.value:
        return PurePosixPath(value)
    elif type == PathType.windows.value:
        return PureWindowsPath(value)
    else:
        raise ValueError(f"Unknown path type: {type}")


def segments_to_path(segments: list[str], type: PathType) -> str:
    if type == PathType.linux.value:
        return PurePosixPath(*segments)
    elif type == PathType.windows.value:
        return PureWindowsPath(*segments)
    else:
        raise ValueError(f"Unknown path type: {type}")


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


@arg("-t", "--type", choices=[PathType.linux.value, PathType.windows.value], help="Path type (linux or windows)")
@arg("-sr", "--search-regex", help="Only return paths matching regex (prefer --search-plain as it's faster)")
@arg("-mo", "--min-occurences", default=1, type=int, help="Minimum occurence of paths")
@arg("-sp", "--search-plain", help="Only return paths containing string")
@arg("-f", "--format", help="Format paths before printing (use {path}, {name} and {dir} as placeholders)")
@arg("--only", choices=["dirs", "files"], help="Only return directories or files")
@arg("-o", "--output", help="Output file (default: stdout)")
@arg("-rt", "--relative-to", help="Return paths as relative to this path")
def query(**kwargs):
    """Query paths"""
    engine = init_engine(kwargs["db"])
    with Session(engine) as session:
        query = session.query(FiPath.value, func.count(FiPath.value).label("weight"))\
            .group_by(FiPath.value, FiPath.type)

        if kwargs["type"] is not None:
            query = query.where(FiPath.type == kwargs["type"])

        if kwargs["only"] is not None:
            query = query\
                .where(FiPath.is_dir == (kwargs["only"] == "dirs"))

        if kwargs["search_regex"] is not None:
            query = query\
                .having(FiPath.value.regexp_match(kwargs["search_regex"]))

        if kwargs["search_plain"] is not None:
            query = query\
                .having(FiPath.value.like(f"%{kwargs['search_plain']}%"))

        query = query\
            .having(text("weight>=:min_weight").bindparams(min_weight=kwargs["min_occurences"]))\
            .distinct()\
            .order_by(text("value asc, weight desc"))

        def transformer(path: PurePath):
            if kwargs["format"] is None:
                return path

            return kwargs["format"]\
                .replace("{path}", path)\
                .replace("{name}", path.name)\
                .replace("{dir}", path.parent)\
                .replace("{ext}", path.suffix)\
                .replace("{stem}", path.stem)

        relative_to_path = value_to_path(
            kwargs["relative_to"], kwargs["type"]) if kwargs["relative_to"] is not None else None
        relative_to_segments = list(
            relative_to_path.parts) if relative_to_path is not None else None

        def relativize(path: PurePath):
            if relative_to_path is None:
                return path

            try:
                relative_path = path.relative_to(relative_to_path)
            except ValueError:
                # need to manually find relative path through components
                path_segments = list(path.parts)
                relative_segments = []
                for i in range(len(relative_to_segments)):
                    if relative_to_segments[i] != path_segments[i]:
                        break
                relative_segments.extend(
                    [".." for _ in range(len(relative_to_segments) - i)])
                relative_segments.extend(path_segments[i:])
                relative_path = segments_to_path(
                    relative_segments, kwargs["type"])

            return relative_path

        output = sys.stdout
        if kwargs["output"] is not None:
            output = open(kwargs["output"], "w")

        count = 0
        for path, _ in query:
            obj = value_to_path(path, kwargs["type"])
            relativized = relativize(obj)
            transformed = transformer(relativized)
            output.write(f"{transformed}\n")
            count += 1

        if count == 0:
            console.print(f"{ERROR} No paths found for given query")

        output.close()


parser = ArghParser()
parser.add_argument("--db", default="sqlite:///db.sqlite",
                    help="Database connection string")
parser.add_commands([unzip, read, query])

if __name__ == "__main__":
    parser.dispatch()
