#!/bin/bash

# Generate $N_COPIES of files based on N_COPIES
#   ./taskA.sh . input

RUN_DIR=$1
INPUT_FILE=$2

mkdir -p $RUN_DIR/intermediate

N_COPIES=$((( RANDOM % 10 ) + 1 ))
for i in `seq 1 $N_COPIES`; do
    cp `pwd`/$INPUT_FILE $RUN_DIR/intermediate/out-a-$i
done
