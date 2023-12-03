package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class MarketReducer extends Reducer<Text, Text, NullWritable, Text> {

    private Text finalRecord = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean IsMarket = false;
        int count = 0;
        String record = "";

        for (Text value: values) {
            String Title = value.toString().split("\t")[0];
            if (Title.equals("Market")){
                IsMarket = true;
                record = value.toString();
            }

            if (Title.equals("Trade")) count++;
        }

        if(IsMarket){
            String[] fields = record.split("\t");
            if (count == 0){
                finalRecord.set(fields[1] + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4] + "\t" +
                        fields[5] + "\t" + fields[6] + "\t" + fields[7] + "\t" + "1");
            } else {
                finalRecord.set(fields[1] + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4] + "\t" +
                        fields[5] + "\t" + fields[6] + "\t" + count + "\t" + "2");
            }
        }

        context.write(NullWritable.get(), finalRecord);
    }

}
