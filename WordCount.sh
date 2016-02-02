confLocation=WordCount.conf &&
dos2unix $confLocation &&
executors=4 &&
memory=2g &&
entry_function=count_words &&
spark-submit \
    --master yarn-client \
    --num-executors $executors \
    --executor-memory $memory \
    WordCount.py $entry_function $confLocation
