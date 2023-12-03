package reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TradedReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String execType = value.toString().split("\t")[7];
            if ("2".equals(execType)) {
                multipleOutputs.write("Traded", NullWritable.get(), value);
            } else {
                multipleOutputs.write("Canceled", NullWritable.get(), value);
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
