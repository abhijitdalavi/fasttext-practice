#!/usr/bin/env bash
export WORK_DIR=/pang/category-prediction-model

PYTHON_DIR=/home/coupang/anaconda3/envs/py36/bin
RUN_DIR=$WORK_DIR/app
LOG_DIR=$WORK_DIR/logs

nohup $PYTHON_DIR/python3 $RUN_DIR/api.py $1 > $LOG_DIR/api.log 2>&1 &
