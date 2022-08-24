#!/bin/bash

url="s3://maap-hec-ades-out-dev/zhan"
key="$1"
secret="$2"
token="$3"
region="us-west-2"
dir="/home/jovyan/output"
file="/home/jovyan/another.txt"

python3 stage_out.py "$url" "$key" "$secret" "$token" "$region" "$dir" "$file"
