package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class OrderReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }


    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values) {
            String OrderType = value.toString().split("\t")[4];
            switch (OrderType){
                case "1":
                    multipleOutputs.write("MarketOrder", NullWritable.get(), value);
                    break;
                case "2":
                    multipleOutputs.write("LimitOrder", NullWritable.get(), value);
                    break;
                case "U":
                    multipleOutputs.write("SpecOrder", NullWritable.get(), value);
                    break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
