package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TradeTypeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String orderID = fields[5];

        outputKey.set(orderID);

        if (fields[7].equals("2")) {
            outputValue.set("Trade\t" + value);
        } else if (fields[7].equals("1")) {
            outputValue.set("Cancel\t" + value);
        }

        context.write(outputKey,outputValue);
    }
}
