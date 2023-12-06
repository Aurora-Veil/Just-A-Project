package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper class for processing original trade data.
 * This class extracts relevant information and filters data based on the project final requests.
 * Input: <LongWritable, Text> - Input key-value pair.
 * Output: <LongWritable, Text> - Output key-value pair with time as the key and filtered trade records as the value.
 */
public class TradeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable outputKey = new LongWritable();
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
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the input value using tab as the delimiter
        String[] fields = value.toString().split("\t");

        // Extract relevant fields from the records
        String securityID = fields[8];
        String tradeTime = fields[15];
        String bidApplSeqNum = fields[10];
        String offerApplSeqNum = fields[11];
        String cancelType = fields[14].equals("4")? "1": "2";

        // Check if the stock ID is "000001" and the time is during continuous trading
        if (isContinuousAuctionTime(tradeTime) && "000001".equals(securityID)){
            // Extract the time in hours for grouping
            long timeForHour = Long.parseLong(tradeTime.substring(tradeTime.length() - 9));

            /*
             Prepare the trade record based on the bidApplSeqNum and offerApplSeqNum
             Records' order are transformed into the final output's format
             The format is: TimeStamp, Price, Size, Buy_Sell_Flag, Order_Type, Order_ID, Market_Order_Type, Cancel_Type
             Unknown and unnecessary are recorded as "NULL", here for the trade are Order_Type and Market_Order_Type
             Output key = time
             Output value = records
             */

            /*
             For one original trade record, we split it into one or two records,that is:
             If the bidApplSeqNum or offerApplSeqNum is efficient (unequal 0), then put it into the space of Order_ID, generate a record
             For example, if the original record is Cancel_Type = F(2), then generates two records, one is for bid with bidApplSeqNum in Order_ID, another is for offer with offerApplSeqNum
                          If the original record is Cancel_Type = 4(1), it only has one efficient AppSeqNum, only generates one record
             */
            if (!bidApplSeqNum.equals("0")){
                outputKey.set(timeForHour);
                String record = fields[15] + "\t" + fields[12] + "\t" + fields[13] + "\t" +
                        "1" + "\t" + "NULL" + "\t" + bidApplSeqNum + "\t" + "NULL" + "\t" + cancelType;
                outputValue.set(record);
                context.write(outputKey,outputValue);
            }
            if (!offerApplSeqNum.equals("0")){
                outputKey.set(timeForHour);
                String record = fields[15] + "\t" + fields[12] + "\t" + fields[13] + "\t" +
                        "2" + "\t" + "NULL" + "\t" + offerApplSeqNum + "\t" + "NULL" + "\t" + cancelType;
                outputValue.set(record);
                context.write(outputKey,outputValue);
            }
        }
    }

    /**
     * Checks if the given time is within continuous trading hours.
     *
     * @param tradeTime The time in the format of TransactTime N(20).
     * @return True if the time is within continuous trading hours: 9:30 - 11:30, 13:00 - 14:57, false otherwise.
     */
    private boolean isContinuousAuctionTime(String tradeTime) {
        // Take the hours and minutes from the String
        int hourMinute = Integer.parseInt(tradeTime.substring(8, 12));

        // Define continuous trading time interval
        int startTime1 = 930;
        int endTime1 = 1130;
        int startTime2 = 1300;
        int endTime2 = 1457;

        // Check if the hourMinute is within the defined trading time
        return (hourMinute >= startTime1 && hourMinute <= endTime1) || (hourMinute >= startTime2 && hourMinute < endTime2);
    }
}