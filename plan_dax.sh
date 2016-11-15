#!/bin/bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 1 ]; then
    echo "Usage: $0 DAXFILE"
    exit 1
fi

DAXFILE=$1

# This command tells Pegasus to plan the workflow contained in 
# dax file passed as an argument. The planned workflow will be stored
# in the "submit" directory. The execution # site is "".
# --input-dir tells Pegasus where to find workflow input files.
# --output-dir tells Pegasus where to place workflow output files.
    #-Dpegasus.register=false \
pegasus-plan \
    -Dpegasus.catalog.site.file=sites.xml \
    -Dpegasus.catalog.transformation.file=tc.txt \
    --sites local \
    --output-dir $DIR/output \
    --dir $DIR/submit \
    --dax $DAXFILE \
    --submit 
