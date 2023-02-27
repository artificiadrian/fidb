from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from argparse import ArgumentParser
from db import Path, to_path
import sys
from rich.console import Console

LOADING = "[*]"
SUCCESS = "[green][+][/green]"
ERROR = "[red][-][/red]"

parser = ArgumentParser()
parser.add_argument("--db", default="sqlite:///db.sqlite")
parser.add_argument("--load", action="store_true")
parser.add_argument("--unzip", type=str)
parser.add_argument("--search", type=str)

args = parser.parse_args()

console = Console()

engine = create_engine(args.db)
Path.metadata.create_all(engine)


def collect_paths(generator):
    count = 0
    with console.status("Adding paths...") as status:
        with Session(engine) as session:
            for path in generator:
                count += 1
                session.add(to_path(path))
                if count % 10_000 == 0:
                    status.update(f"Adding paths... ({count} so far)")
                if count % 100_000 == 0:
                    session.commit()
    console.print(f"{SUCCESS} Added {count} paths")


if args.unzip:
    import zipfile
    import io

    console.print(f"{LOADING} Adding paths from zip...")
    with zipfile.ZipFile(args.unzip) as f:
        collect_paths(io.TextIOWrapper(f.open("paths.txt")))
elif args.load:
    console.print(f"{LOADING} Adding paths from stdin...")
    collect_paths(sys.stdin)
elif args.search:
    with Session(engine) as session:
        for path in session.query(Path).filter(Path.value.like(f"%{args.search}%")):
            console.print(path.value)
