package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] records = value.toString().split("\t");

        String ApplSeqNum = records[7];
        String id = records[8];
        String time = records[12];

        if (id.equals("000001") && isInContinuousTrading(time)){
            outputKey.set(ApplSeqNum);
            String orderRecord = records[12] + "\t" + records[10] + "\t" + records[11] + "\t" + records[13] + "\t"
                    + records[14] + "\t" + records[7] + "\t" + "NULL" + "\t" + "NULL";
            outputValue.set(orderRecord);
            context.write(outputKey, outputValue);
        }
    }

    private boolean isInContinuousTrading(String time) {
        int hourMinute = Integer.parseInt(time.substring(8, 12));

        int startTime1 = 930;
        int endTime1 = 1130;
        int startTime2 = 1300;
        int endTime2 = 1457;
        return (hourMinute >= startTime1 && hourMinute <= endTime1) || (hourMinute >= startTime2 && hourMinute < endTime2);
    }
}
