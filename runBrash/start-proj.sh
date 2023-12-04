#!/bin/bash

start_time=$(date +%s)

# Step 1: Run OrderDriver
hadoop jar mapreduce-lab8-1.0-SNAPSHOT.jar driver.OrderDriver data/am_hq_order_spot.txt data/pm_hq_order_spot.txt MiddleOutput1

# Step 2: Run TradeDriver
hadoop jar mapreduce-lab8-1.0-SNAPSHOT.jar driver.TradeDriver data/am_hq_trade_spot.txt data/pm_hq_trade_spot.txt MiddleOutput2

# Step 3: Run MarketDriver
hadoop jar mapreduce-lab8-1.0-SNAPSHOT.jar driver.MarketDriver MiddleOutput1/MarketOrder-r-00000 MiddleOutput2/Traded-r-00000 MiddleOutput3

# Step 4: Run CancelDriver
hadoop jar mapreduce-lab8-1.0-SNAPSHOT.jar driver.CancelDriver MiddleOutput1/LimitOrder-r-00000 MiddleOutput1/part-r-00000 MiddleOutput2/Canceled-r-00000 MiddleOutput4

# Step 5: Run FinalJoin
hadoop jar mapreduce-lab8-1.0-SNAPSHOT.jar driver.FinalJoin MiddleOutput3/part-r-00000 MiddleOutput4/LimitOrder-r-00000 MiddleOutput4/Cancel-r-00000 MiddleOutput4/part-r-00000 FinalOutput

hdfs dfs -mv FinalOutput/part-r-00000 Output.txt

end_time=$(date +%s)

execution_time=$((end_time - start_time))

minutes=$((execution_time / 60))
seconds=$((execution_time % 60))

echo "Script execution time: ${minutes} minutes and ${seconds} seconds."
