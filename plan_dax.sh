#!/bin/bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -ne 1 ]; then
    echo "Usage: $0 DAXFILE"
    exit 1
fi

DAXFILE=$1

# This command tells Pegasus to plan the workflow contained in 
# dax file passed as an argument. The planned workflow will be stored
# in the "submit" directory.
pegasus-plan \
    -Dpegasus.transfer.links=true \
    -Dpegasus.catalog.site.file=sites.xml \
    -Dpegasus.catalog.transformation.file=tc.txt \
    -Dpegasus.data.configuration=sharedfs \
    --sites lsstvc \
    --output-dir $DIR/output \
    --dir $DIR/submit \
    --dax $DAXFILE \
    --submit 
