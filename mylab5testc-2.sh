#!/bin/bash
export RPC_COUNT=5
./start.sh
./test-lab-3-c ./yfs1 ./yfs2

./stop.sh
./stop.sh
export RPC_COUNT=0