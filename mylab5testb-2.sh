#!/bin/bash
export RPC_COUNT=5
./start.sh
./test-lab-3-b ./yfs1 ./yfs2

./stop.sh
./stop.sh
