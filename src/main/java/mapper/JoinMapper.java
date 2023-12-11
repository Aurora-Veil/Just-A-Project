package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for processing original trade data.
 * This class extracts relevant information and filters data based on the project final requests.
 * Input: <LongWritable, Text> - Input key-value pair.
 * Output: <LongWritable, Text> - Output key-value pair, time as the key and filtered trade records as the value.
 */

public class JoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

     private LongWritable outKey = new LongWritable();
     private Text outValue = new Text();
    /**
     * Maps input key-value pairs to intermediate key-value pairs.
     *
     * @param key     The input key.
     * @param value   The input value.
     * @param context The context object for emitting output.
     *                The output key is time, output value is composed by those useful fields in the original record in the format of final output.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the task is interrupted.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        String time = fields[0];

        String timeValues = time.substring(11, 13) + time.substring(14, 16) + time.substring(17, 19) + time.substring(20, 23);

        long timePerHour = Long.parseLong(timeValues);

        outKey.set(timePerHour);
        outValue.set(value);

        context.write(outKey, outValue);
    }
}
