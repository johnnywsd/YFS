#!/bin/bash
grep 'lock_client_cache' yfs_client1.log >> yfs_client1-2.log
grep 'lock_client_cache' yfs_client2.log >> yfs_client2-2.log
