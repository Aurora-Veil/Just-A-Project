package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 解析逐笔成交数据记录
        String[] fields = value.toString().split("\t");

        // 获取证券代码
        String securityID = fields[7];

        // 获取成交时间
        String tradeTime = fields[15];

        // 获取成交类别
        String execType = fields[19];

        // 过滤连续竞价时间段
        if (!isContinuousAuctionTime(tradeTime)) {
            // 判断是否为平安银行
            if ("000001".equals(securityID)) {
                // 提取所需字段，拼接为输出值
                String output = fields[8] + "," + fields[10] + "," + fields[11] + "," +
                        fields[12] + "," + fields[13] + "," + fields[14] + "," + fields[15];

                // 设置输出键值对
                outputKey.set(securityID);

                // 根据成交类别分发到不同的Reducer
                if ("4".equals(execType)) {
                    // 撤单
                    context.write(new Text("cancle"), new Text(output));
                } else if ("F".equals(execType)) {
                    // 成交
                    context.write(new Text("traded"), new Text(output));
                }
            }
        }
    }

    private boolean isContinuousAuctionTime(String tradeTime) {
        int hourMinute = Integer.parseInt(tradeTime.substring(8, 12));
        int startTime1 = 930;
        int endTime1 = 1130;
        int startTime2 = 1300;
        int endTime2 = 1457;
        return (hourMinute >= startTime1 && hourMinute <= endTime1) || (hourMinute >= startTime2 && hourMinute < endTime2);
    }
}