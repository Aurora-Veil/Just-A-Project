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
        //买方委托索引
        String bidApplSeqNum = fields[10];
        //卖方委托索引
        String offerApplSeqNum = fields[11];

        String outputKeyString = bidApplSeqNum + "," + offerApplSeqNum;

        // 过滤连续竞价时间段
        if (!isContinuousAuctionTime(tradeTime)&&"000001".equals(securityID)){
            // 判断是否为平安银行

                // 提取所需字段，拼接为输出值
                String output = fields[8] + "\t" + fields[10] + "\t" + fields[11] + "\t" +
                        fields[12] + "\t" + fields[13] + "\t" + fields[14] + "\t" + fields[15];

                // 设置输出键值对
            outputKey.set(outputKeyString);
                outputValue.set(output);
                context.write(outputKey, outputValue);

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