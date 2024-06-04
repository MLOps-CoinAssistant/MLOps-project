#!/bin/bash

# 첫째줄은 주석이 아니라 shebang라인이라고 불리는 스크립트 실행에 사용할 인터프리터를 지정하는 구문임
# 첫쨰줄에 작성해야한다고 하니 삭제하지말것.

# 태스크 디버깅
# echo "TASK: $TASK"
# echo "FUNCTION: $FUNCTION"
# Load environment variables from file if it exists
# if [ -f /tmp/env_vars ]; then
#     source /tmp/env_vars
# else
#     echo "/tmp/env_vars file not found"
#     exit 1
# fi

# # Print all environment variables for debugging
# echo "Current environment variables:"
# env

# # Print the TASK and FUNCTION variables for debugging
# echo "TASK: $TASK"
# echo "FUNCTION: $FUNCTION"
TASK=$TASK
FUNCTION=$FUNCTION

echo "TASK: $TASK"
echo "FUNCTION: $FUNCTION"

if [ "$TASK" == "classification_task" ]; then
    pip install --no-cache-dir -r requirements_classification.txt
    python -c "from classification import $FUNCTION; $FUNCTION()"
elif [ "$TASK" == "create_table_task" ]; then
    python -c "from create_table import $FUNCTION; $FUNCTION()"
elif [ "$TASK" == "preprocess_task" ]; then
    pip install --no-cache-dir -r requirements_preprocess.txt
    python -c "from preprocess import $FUNCTION; $FUNCTION()"
elif [ "$TASK" == "save_raw_data_task" ]; then
    pip install --no-cache-dir uvloop==0.17.0
    python -c "import uvloop; uvloop.install()" # asyncio의 이벤트루프를 uvloop로 설치하기
    pip install --no-cache-dir -r requirements_save_raw_data.txt
    python -c "from save_raw_data import $FUNCTION; $FUNCTION()"
else
    echo "Unknown task"
    exit 1
fi
