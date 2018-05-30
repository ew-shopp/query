#!/bin/bash
export DATE=`date +%F_%H%M`
python -u ./query6.py > query6_${DATE}.log 2>&1

