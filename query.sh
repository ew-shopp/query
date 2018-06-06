#!/bin/bash
export DATE=`date +%F_%H%M`
python -u ./query.py > query_${DATE}.log 2>&1

