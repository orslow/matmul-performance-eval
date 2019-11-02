#!/bin/bash

# prepare .e and .v file on same directory
# ./run.sh filename (without file type)

python gen-n.py $1
echo "unsorted random N made"
python sort.py $1
echo "wait for parsing..."
echo "done!"

