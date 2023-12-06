//package mapper;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//
///**
// * Mapper class to categorize orders based on their order types.
// * This class is used on MarketOrder, LimitOrder and SpecOrder which are all order records that came from the first step
// * Input: <LongWritable, Text> - Input key-value pair.
// * Output: <Text, Text> - Output key-value pair with order ID as the key and categorized order details as the value.
// */
//public class OrderTypeMapper extends Mapper<LongWritable, Text, Text, Text> {
//
//    private Text outKey = new Text();
//    private Text outValue = new Text();
//
//    /**
//     * Map method to categorize orders based on their order types.
//     * @param key Input key (unused in this context).
//     * @param value Input value containing order details.
//     * @param context Context object for writing output.
//     * @throws IOException If an I/O error occurs.
//     * @throws InterruptedException If the execution is interrupted.
//     */
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        // Split the input value using tab as the delimiter
//        String[] fields = value.toString().split("\t");
//
//        // Extract order ID and order type from the fields
//        String orderID = fields[5];
//        String orderType = fields[4];
//
//        // Set the output key as the order ID
//        outKey.set(orderID);
//
//        // Categorize orders based on order type and set the output value accordingly
//        // Add the orderType at the head of value as identifier
//        switch (orderType){
//            case "1":
//                outValue.set("Market\t" + value);
//                break;
//            case "2":
//                outValue.set("Limit\t" + value);
//                break;
//            case "U":
//                outValue.set("Spec\t" + value);
//                break;
//        }
//
//        /*
//         Emit the output key-value pair
//         key = order id
//         value = identifier + record
//         */
//        context.write(outKey,outValue);
//    }
//}
