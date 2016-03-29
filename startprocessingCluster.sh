spark-submit \
        --class org.mazerunner.extension.SparkApplication \
        --master yarn://sparkcluster-m \
        --executor-memory 10g \
        --executor-cores 4 \
        --num-executors 6 \
        spark-1.1.5-RELEASE-driver.jar \
        "TEST" 12 "/user/k_ponthai/dataSparse/FullGraphSparse100k.txt" "ALL" "result.txt"
