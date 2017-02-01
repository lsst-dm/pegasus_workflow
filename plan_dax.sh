#!/bin/bash

DIR=$(cd $(dirname $0) && pwd)

if [ $# -lt 1 ]; then
    echo "Usage: $0 DAXFILE [SITE] [TCFILE]"
    exit 1
fi

DAXFILE=$1
SITE=${2:-"lsstvc"}
TCFILE=${3:-"tc.txt"}
echo "Planning Pegasus with $DAXFILE and $TCFILE on $SITE"

# This command tells Pegasus to plan the workflow contained in 
# dax file passed as an argument. The planned workflow will be stored
# in the "submit" directory.
pegasus-plan \
    -Dpegasus.transfer.links=true \
    -Dpegasus.catalog.site.file=sites.xml \
    -Dpegasus.catalog.transformation.file=$TCFILE \
    -Dpegasus.data.configuration=sharedfs \
    --sites $SITE \
    --output-dir $DIR/output \
    --dir $DIR/submit \
    --dax $DAXFILE \
    --submit 
