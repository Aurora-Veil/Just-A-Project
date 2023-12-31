#!/bin/bash

start_time=$(date +%s)

hadoop jar Project.jar driver.DoAll data/am_hq_order_spot.txt data/pm_hq_order_spot.txt data/am_hq_trade_spot.txt data/pm_hq_trade_spot.txt MiddleOutput

hadoop jar Project.jar driver.FinalJoin MiddleOutput/part-r-00000 MiddleOutput/LimitOrder-r-00000 MiddleOutput/Cancel-r-00000 MiddleOutput/MarketOrder-r-00000 FinalOutput

hdfs dfs -mv FinalOutput/part-r-00000 you.csv

end_time=$(date +%s)

execution_time=$((end_time - start_time))

minutes=$((execution_time / 60))
seconds=$((execution_time % 60))

echo "Script execution time: ${minutes} minutes and ${seconds} seconds."
