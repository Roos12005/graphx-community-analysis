spark-submit \
        --class org.mazerunner.extension.SparkApplication \
        --master local[*] \
        --driver-memory 30g \
        spark-1.1.5-RELEASE-driver.jar \
        "TEST" 1 "dataSparse/FullGraphSparse5k.txt" "BC" "result.txt"
