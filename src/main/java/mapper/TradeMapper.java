package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TradeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String securityID = fields[8];
        String tradeTime = fields[15];
        String bidApplSeqNum = fields[10];
        String offerApplSeqNum = fields[11];
        String cancelType = fields[14].equals("4")? "1": "2";

        if (isContinuousAuctionTime(tradeTime) && "000001".equals(securityID)){
            if (!bidApplSeqNum.equals("0")){
                outputKey.set(tradeTime);
                String record = fields[15] + "\t" + fields[12] + "\t" + fields[13] + "\t" +
                        "NULL" + "\t" + "NULL" + "\t" + bidApplSeqNum + "\t" + "NULL" + "\t" + cancelType;
                outputValue.set(record);
                context.write(outputKey,outputValue);
            }
            if (!offerApplSeqNum.equals("0")){
                outputKey.set(tradeTime);
                String record = fields[15] + "\t" + fields[12] + "\t" + fields[13] + "\t" +
                        "NULL" + "\t" + "NULL" + "\t" + offerApplSeqNum + "\t" + "NULL" + "\t" + cancelType;
                outputValue.set(record);
                context.write(outputKey,outputValue);
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