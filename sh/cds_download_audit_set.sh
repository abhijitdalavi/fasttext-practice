#!/usr/bin/env bash
export PYTHONPATH=/pang/category-prediction-model

WORK_DIR=$PYTHONPATH/app
DATA_DIR=$PYTHONPATH/data/audit/temp


rm -rf $DATA_DIR/*
/usr/local/bin/aws s3 cp s3://s3-cdp-prod-hive/temp/category_predict/train_candidate/random_result $DATA_DIR --recursive

pushd $DATA_DIR
if [ -e _SUCCESS ]
then
    echo "get cds data success, will start prediction"
else
    echo "failed to get cds data, exit.."
    exit 1
fi
popd

/home/coupang/anaconda3/envs/py36/bin/python3 $WORK_DIR/source.py merge_audit_data $1