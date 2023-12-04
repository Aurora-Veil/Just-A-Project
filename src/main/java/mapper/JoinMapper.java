package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

     private LongWritable outKey = new LongWritable();
     private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String time = fields[0];

        long timePerHour = Long.parseLong(time.substring(time.length() - 9));

        outKey.set(timePerHour);
        outValue.set(value);

        context.write(outKey, outValue);
    }
}
