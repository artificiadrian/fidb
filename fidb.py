#!/usr/bin/env python3

import os
from typing import Optional
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import Session
from argh import ArghParser, arg
from db import Category, FiPath, PathType, get_category, to_path
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


def add_paths_from_generator(generator, engine, category_id: Optional[int] = None):
    count = 0

    with Session(engine) as session:
        for path_str in generator:
            count += 1
            session.add(to_path(path_str, category_id))
            yield count
            if count % 100_000 == 0:
                session.commit()
        session.commit()
    console.print(f"{SUCCESS} Added {count} paths")

def get_category_id(engine, name: str, create: bool=False) -> Category:
    if name is None:
        return None
    with Session(engine) as session:
        category = get_category(session, name)
        if category is None and not create:
            console.print(f"{ERROR} Category {name} does not exist")
            exit(1)
        elif category is None:
            category = Category(name=name)
            session.add(category)
            session.commit()
        return category.id

@arg("file", help="Zip file containing path list file (first file in zip, one path per line)")
@arg("-c", "--category", help="Category to add paths to", default=None)
def unzip(file: str, **kwargs):
    """Read paths from zip"""
    import zipfile
    import io

    engine = init_engine(kwargs["db"])
    category_id = get_category_id(engine, kwargs["category"], True)

    with zipfile.ZipFile(file) as f:
        msg = "Adding paths from zip (%d so far)..."
        with console.status(msg % 0) as status:
            for i in add_paths_from_generator(io.TextIOWrapper(f.open(f.filelist[0])),
                                              engine, category_id):
                if i % 10_000 == 0:
                    status.update(msg % i)


@arg("-c", "--category", help="Category to add paths to", default=None)
def read(**kwargs):
    """Read paths from stdin"""

    engine = init_engine(kwargs["db"])
    category_id = get_category_id(engine, kwargs["category"], True)

    console.print(f"{LOADING} Adding paths from stdin...")

    def generator():
        for line in sys.stdin:
            if line.strip() == "":
                break
            yield line.strip()

    for _ in add_paths_from_generator(generator(), engine, category_id):
        pass


@arg("-t", "--type", choices=[PathType.linux.value, PathType.windows.value], help="Path type (linux or windows)", required=True)
@arg("-sr", "--search-regex", help="Only return paths matching regex (prefer --search-plain as it's faster)")
@arg("-mo", "--min-occurences", default=1, type=int, help="Minimum occurence of paths")
@arg("-sp", "--search-plain", help="Only return paths containing string")
@arg("-f", "--format", help="Format paths before printing (use {path}, {name} and {dir} as placeholders)")
@arg("--only", choices=["dirs", "files"], help="Only return directories or files")
@arg("-o", "--output", help="Output file (default: stdout)")
@arg("-rt", "--relative-to", help="Return paths as relative to this path")
@arg("-c", "--category", help="Category to query", default=None)
def query(**kwargs):
    """Query paths"""
    engine = init_engine(kwargs["db"])
    with Session(engine) as session:
        query = session.query(FiPath.value, func.count(FiPath.value).label("weight"))\
            .group_by(FiPath.value, FiPath.type)
        
        category_id = get_category_id(engine, kwargs["category"])

        if kwargs["type"] is not None:
            query = query.where(FiPath.type == kwargs["type"])

        if kwargs["only"] is not None:
            query = query\
                .where(FiPath.is_dir == (kwargs["only"] == "dirs"))
            
        if category_id is not None:
            query = query.where(FiPath.category_id == category_id)

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
