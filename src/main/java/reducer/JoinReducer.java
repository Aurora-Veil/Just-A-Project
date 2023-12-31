package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reducer class is typically responsible for aggregating, summarizing, or otherwise processing the intermediate data generated by the Mapper.
 * This Reducer is mainly used to summarize and sort JoinMapper data to form the final output
 * Input: <LongWritable, Text> - Input key-value pair.
 * Output: <NullWritable, Text> - Output key-value pair with NULL key and records as the value.
 */
public class JoinReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    /**
     * Reduce intermediate key-value pairs to final key-value pairs.
     *
     * @param key     The input key.
     * @param values  The input value.
     * @param context The context object for emitting output.
     *                The output key is NULL, output value is composed by those useful fields in the intermediate record in the format of final output.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the task is interrupted.
     */
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<DataRecord> records = new ArrayList<>();

        for (Text value : values) {
            String[] fields = value.toString().split(",");
            String sequenceNumber = fields[5];
            records.add(new DataRecord(sequenceNumber, value.toString()));
        }

        // sort the records by  applSeqNum
        Collections.sort(records);

        for (DataRecord record : records) {
            context.write(NullWritable.get(), new Text(record.getData()));
        }
    }

    /**
     * This is a nested class representing a data record. It has a sequenceNumber and data field.
     * A constructor to initialize the fields and a method to retrieve the data.
     * It implements the Comparable interface to define a custom sorting order based on the sequenceNumber.
     * This is used for sorting the records in ascending order.
     */
    private static class DataRecord implements Comparable<DataRecord> {
        private final String sequenceNumber;
        private final String data;

        public DataRecord(String sequenceNumber, String data) {
            this.sequenceNumber = sequenceNumber;
            this.data = data;
        }

        public String getData() {
            return data;
        }

        /**
         * The compareTo method compares based on the sequenceNumber field in ascending order.
         */
        @Override
        public int compareTo(DataRecord other) {
            return Integer.compare(Integer.parseInt(this.sequenceNumber), Integer.parseInt(other.sequenceNumber));
        }
    }
}
