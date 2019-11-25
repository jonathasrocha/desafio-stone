#!/bin/bash -e
aws s3 cp ../data s3://$bucket_name/data --recursive
