package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JoinReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        List<DataRecord> records = new ArrayList<>();

        // Iterate through values and store in a list
        for (Text value : values) {
            String[] fields = value.toString().split("\t");
            String sequenceNumber = fields[5];
            records.add(new DataRecord(sequenceNumber, value.toString()));
        }

        // Custom sorting based on time and sequence number
        Collections.sort(records);

        // Output the sorted records
        for (DataRecord record : records) {
            context.write(NullWritable.get(), new Text(record.getData()));
        }
    }

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

        @Override
        public int compareTo(DataRecord other) {
            return Integer.compare(Integer.parseInt(this.sequenceNumber), Integer.parseInt(other.sequenceNumber));
        }
    }
}
