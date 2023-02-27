import os
from argparse import ArgumentParser
import re
from zipfile import ZIP_DEFLATED


parser = ArgumentParser()
parser.add_argument("--root", default=os.path.abspath(os.sep))
parser.add_argument("--output", default="paths.txt")
parser.add_argument("--zip", action="store_true", default=True)
parser.add_argument("--omit")

args = parser.parse_args()


count = 0

print("[*] Harvesting paths...")
with open(args.output, "w") as f:
    for root, dirs, files in os.walk(args.root):
        for path in [*[os.path.join(root, f) for f in files], *[os.path.join(root, d) + os.sep for d in dirs]]:
            if re.match(args.omit, path) is not None:
                continue
            try:
                f.write(path + "\n")
                count += 1
                if count % 100_000 == 0:
                    print(f"[*] Harvested {count} paths")
            except:
                pass

print(f"[+] Harvested {count} paths")

if args.zip:
    print("[*] Zipping...")
    import zipfile

    zip_name = args.output + ".zip"
    with zipfile.ZipFile(zip_name, "w", compression=ZIP_DEFLATED) as f:
        f.write(args.output)
    os.remove(args.output)
    args.output = zip_name

print(f"[+] Saved to {args.output}")
