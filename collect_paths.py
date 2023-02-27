import os
from argparse import ArgumentParser
from zipfile import ZIP_DEFLATED
from rich.console import Console

parser = ArgumentParser()
parser.add_argument("--root", default=os.path.abspath(os.sep))
parser.add_argument("--output", default="paths.txt")
parser.add_argument("--zip", action="store_true", default=True)

args = parser.parse_args()

console = Console()

count = 0

with (console.status("Collecting paths...")) as status:
    with open(args.output, "w") as f:
        for root, dirs, files in os.walk(args.root):
            for path in [*[os.path.join(root, f) for f in files], *[os.path.join(root, d) + os.sep for d in dirs]]:
                try:
                    f.write(path + "\n")
                    count += 1
                except:
                    pass

console.print(f"[*] Collected {count} paths")

if args.zip:
    with console.status("Zipping...") as status:
        import zipfile

        zip_name = args.output + ".zip"
        with zipfile.ZipFile(zip_name, "w", compression=ZIP_DEFLATED) as f:
            f.write(args.output)
        os.remove(args.output)
        args.output = zip_name

console.print(f"[+] Saved to {args.output}")
