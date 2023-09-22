#!/usr/bin/env bash

cd /app
source $(poetry env info --path)/bin/activate
/opt/bitnami/spark/bin/spark-submit /app/src/jobs/job.py