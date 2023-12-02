package reducer;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TradedReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values) {
            String execType = value.toString().split("\t")[5];
            if ("F".equals(execType)) {
                multipleOutputs.write("Traded", key, new Text("1")); // "1" 表示成交记录
            } else {
                multipleOutputs.write("Canceled", key, new Text("0")); // "0" 表示撤单记录
            }
        }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
