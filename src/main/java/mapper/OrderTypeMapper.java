package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderTypeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String orderID = fields[5];
        String orderType = fields[4];

        outKey.set(orderID);

        switch (orderType){
            case "1":
                outValue.set("Market\t" + value);
                break;
            case "2":
                outValue.set("Limit\t" + value);
                break;
            case "U":
                outValue.set("Spec\t" + value);
                break;
        }

        context.write(outKey,outValue);
    }
}
