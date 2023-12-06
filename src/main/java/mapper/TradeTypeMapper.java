package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class to categorize trades based on their cancel type.
 * This class is used on Traded, Canceled which are all trade records that came from the first step
 * Input: <LongWritable, Text> - Input key-value pair.
 * Output: <Text, Text> - Output key-value pair with order ID as the key and categorized trade details as the value.
 */
public class TradeTypeMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    /**
     * Map method to categorize orders based on their order types.
     * @param key Input key (unused in this context).
     * @param value Input value containing order details.
     * @param context Context object for writing output.
     * @throws IOException If an I/O error occurs.
     * @throws InterruptedException If the execution is interrupted.
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input value using tab as the delimiter
        String[] fields = value.toString().split("\t");
        String orderID = fields[5];

        // Set the output key as the order ID
        outputKey.set(orderID);

        // Categorize orders based on cancel type and set the output value accordingly
        // Add the cancelType at the head of value as identifier
        if (fields[7].equals("2")) {
            outputValue.set("Trade\t" + value);
        } else if (fields[7].equals("1")) {
            outputValue.set("Cancel\t" + value);
        }

        /*
         Emit the output key-value pair
         key = order id
         value = identifier + record
         */
        context.write(outputKey,outputValue);
    }
}
