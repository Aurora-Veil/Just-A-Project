package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for processing original order data.
 * This class extracts relevant information and filters data based on the project final requests.
 * Input: <LongWritable, Text> - Input key-value pair.
 * Output: <LongWritable, Text> - Output key-value pair with applSeqNum as the key and filtered order records as the value.
 */
public class OrderMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();


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
        // Split the input value using tab as the delimiter
        String[] records = value.toString().split("\t");

        // Extract relevant fields from the records
        String applSeqNum = records[7];
        String id = records[8];
        String time = records[12];
        String orderType = records[14];

        // Check if the stock ID is "000001" and the time is during continuous trading
        if (id.equals("000001") && isInContinuousTrading(time)){
            // Extract the time in hours for grouping
            outputKey.set(applSeqNum);

            String changeTime = String.format("%s-%s-%s %s:%s:%s.%s000",
                    time.substring(0, 4),   // year
                    time.substring(4, 6),   // month
                    time.substring(6, 8),   // day
                    time.substring(8, 10),  // hour
                    time.substring(10, 12), // minute
                    time.substring(12, 14), // seconds
                    time.substring(14)      // millisecond
            );

            /*
             Prepare the order record based on the order type
             Here records' order are transformed into the final output's format
             The format is: TimeStamp, Price, Size, Buy_Sell_Flag, Order_Type, Order_ID, Market_Order_Type, Cancel_Type
             Unknown and unnecessary are recorded as "NULL", here for the order are Market_Order_Type and Cancel_Type
             */
            String orderRecord = "";
            switch (orderType) {
                case "2":
                    orderRecord = "Limit" + "," + changeTime + "," + records[10] + "," + records[11] + "," + records[13] + ","
                            + records[14] + "," + records[7] + "," + "NULL" + "," + "2";
                    break;
                case "1":
                    orderRecord = "Market" + "," + changeTime + "," + "NULL" + "," + records[11] + "," + records[13] + ","
                            + records[14] + "," + records[7] + "," + "NULL" + "," + "2";
                    break;
                case "U":
                    orderRecord = "Spec" + "," + changeTime + "," + "NULL" + "," + records[11] + "," + records[13] + ","
                            + records[14] + "," + records[7] + "," + "NULL" + "," + "2";
                    break;
            }
            outputValue.set(orderRecord);

            /*
             Emit the output key-value pair
             Output key = id
             Output value = identifier + record
             */
            context.write(outputKey, outputValue);
        }
    }

    /**
     * Checks if the given time is within continuous trading hours.
     *
     * @param time The time in the format of TransactTime N(20).
     * @return True if the time is within continuous trading hours: 9:30 - 11:30, 13:00 - 14:57, false otherwise.
     */
    private boolean isInContinuousTrading(String time) {
        // Take the hours and minutes from the String
        int hourMinute = Integer.parseInt(time.substring(8, 12));

        // Define continuous trading time interval
        int startTime1 = 930;
        int endTime1 = 1130;
        int startTime2 = 1300;
        int endTime2 = 1457;

        // Check if the hourMinute is within the defined trading time
        return (hourMinute >= startTime1 && hourMinute <= endTime1) || (hourMinute >= startTime2 && hourMinute < endTime2);
    }
}
