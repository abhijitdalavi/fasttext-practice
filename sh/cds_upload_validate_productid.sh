#!/usr/bin/env bash
export PYTHONPATH=/pang/category-prediction-model

WORK_DIR=$PYTHONPATH/app
PRODUCT_ID_DIR=$PYTHONPATH/data/raw/temp


/home/coupang/anaconda3/envs/py36/bin/python3 $WORK_DIR/source.py get_validate_product_id

/usr/local/bin/aws s3 rm s3://s3-cdp-prod-hive/temp/category_predict/train_candidate/product_id/ --recursive
/usr/local/bin/aws s3 cp $PRODUCT_ID_DIR s3://s3-cdp-prod-hive/temp/category_predict/train_candidate/product_id/ --exclude "*" --include "product_id_list_for_cds_retrieval_validate.csv" --recursive