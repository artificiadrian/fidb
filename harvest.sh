#!/bin/sh
# Usage: harvest.sh <directory> <output file>
if [ $# -lt 2 ]; then
    echo "Usage: $0 <directory> <output file>"
    exit 1
fi
find $1 -type d -exec sh -c 'echo "$1/"' _ {} \; > $2
find $1 -type f >> $2
zip -r $2.zip $2