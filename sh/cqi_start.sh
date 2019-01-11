#!/usr/bin/env bash
export PYTHONPATH=/pang/category-prediction-model

WORK_DIR=$PYTHONPATH/app
OUTPUT_DIR=$PYTHONPATH/data/cqi/output
INPUT_DIR=$PYTHONPATH/data/cqi/input

DATE=`date +%Y%m%d`
echo $DATE

pushd $INPUT_DIR

COUNT=0
MAX_TRIES=4
INPUT_FILES=0

while [  $COUNT -lt $MAX_TRIES ]; do

    rm -rf $INPUT_DIR/*
    /usr/local/bin/aws s3 cp s3://s3-cdp-prod-hive/temp/category_predict/candidate . --recursive

    if [ -e _SUCCESS ]
    then
        echo "get input data success, will start prediction"
        INPUT_FILES=`ls -1q $INPUT_DIR/part* | wc -l`
        echo $INPUT_FILES
        break
    else
        let COUNT=COUNT+1
        if [ $COUNT -ge $MAX_TRIES ]
        then
            echo "max try attempt exceeded. exit"
            exit 1
        else
            echo "failed to get input data...trying again..."
            sleep 30m
        fi
    fi
done

popd

rm -rf $OUTPUT_DIR/*

/home/coupang/anaconda3/envs/py36/bin/python3 $WORK_DIR/source.py +
/home/coupang/anaconda3/envs/py36/bin/python3 $WORK_DIR/cqi.py overwrite

pushd $OUTPUT_DIR

if [ -e _SUCCESS ]
then
   echo "cqi category prediction processing is success"
else
   echo "cqi category prediction processing failed"
   exit 1
fi

/usr/local/bin/aws s3 rm s3://s3-cdp-prod-hive/temp/category_predict/model_result/dt=$DATE/ --recursive
/usr/local/bin/aws s3 cp $OUTPUT_DIR s3://s3-cdp-prod-hive/temp/category_predict/model_result/dt=$DATE --exclude "*" --include "*.avro" --recursive
UPLOAD_FILES=`/usr/local/bin/aws s3 ls s3://s3-cdp-prod-hive/temp/category_predict/model_result/dt=$DATE --recursive | wc -l`

if [ $UPLOAD_FILES -eq $INPUT_FILES ]
then
    echo "Upload files successful."
    /usr/local/bin/aws s3 cp $OUTPUT_DIR s3://s3-cdp-prod-hive/temp/category_predict/model_result/dt=$DATE --exclude "*" --include "_SUCCESS" --recursive
else
    echo "Upload files ERROR!!!"
fi



popd