package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Reducer class is typically responsible for aggregating, summarizing, or otherwise processing the intermediate data generated by the Mapper.
 * This class handle the aggregation and organization of different types of orders based on the information provided by the Mapper.
 * Input: <Text, Iterable<Text>> - Input key-value pair.
 * Output: <NullWritable, Text> - Output key-value pair with NULL key and filtered trade records as the value.
 */

public class DoAllReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private Text outMarketValue = new Text();
    private Text outOrderValue = new Text();
    private Text outCancelValue = new Text();

    /**
     * Reduce input key-value pairs to intermediate key-value pairs.
     *
     * @param key     The input key.
     * @param values  The input value.
     * @param context The context object for emitting output.
     *                The output key is time, output value is composed by those useful fields in the original record in the format of final output.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the task is interrupted.
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean IsCancel = false;
        boolean IsLimit = false;
        boolean IsSpec = false;
        boolean IsMarket = false;
        int count = 0;

        String Limit = "";
        String Spec = "";
        String Cancel = "";
        String Market = "";

        /*
          The purpose of this code is to categorize data based on the order types in the dataset.
          It sets corresponding flags (`IsCancel`, `IsSpec`, `IsLimit`, `IsMarket`),
          stores values of respective types (`Cancel`, `Spec`, `Limit`, `Market`).
          In the subsequent code, further processing and output are performed based on these flags and stored values.
         */
        for (Text value : values) {
            String Title = value.toString().split("\t")[0];
            switch (Title) {
                case "Cancel":
                    IsCancel = true;
                    Cancel = value.toString();
                    break;
                case "Spec":
                    IsSpec = true;
                    Spec = value.toString();
                    break;
                case "Limit":
                    IsLimit = true;
                    Limit = value.toString();
                    break;
                case "Market":
                    IsMarket = true;
                    Market = value.toString();
                    break;
                case "Trade":
                    count++;
                    break;
            }
        }
        /*
        The following performs the final processing of different types of orders.
        Writes the results to distinct output files based on the order type and according to different case.
         */
        if (IsMarket) {
            String[] fields = Market.split("\t");
            if (count == 0) {
                outMarketValue.set(fields[1] + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4] + "\t" +
                        fields[5] + "\t" + fields[6] + "\t" + count + "\t" + "1");
            } else {
                outMarketValue.set(fields[1] + "\t" + fields[2] + "\t" + fields[3] + "\t" + fields[4] + "\t" +
                        fields[5] + "\t" + fields[6] + "\t" + count + "\t" + "2");
            }
            multipleOutputs.write("MarketOrder", NullWritable.get(), outMarketValue);
        }

        String[] limits = Limit.split("\t");
        String[] specs = Spec.split("\t");

        if (IsCancel) {
            String[] cancels = Cancel.split("\t");
            if (IsLimit) {
                outOrderValue.set(limits[1] + "\t" + limits[2] + "\t" + limits[3] + "\t" + limits[4] + "\t" +
                        limits[5] + "\t" + limits[6] + "\t" + limits[7] + "\t" + "1");
                multipleOutputs.write("LimitOrder", NullWritable.get(), outOrderValue);
                outCancelValue.set(cancels[1] + "\t" + "NULL" + "\t" + cancels[3] + "\t" + "NULL" + "\t" +
                        "2" + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            } else if (IsSpec) {
                outOrderValue.set(specs[1] + "\t" + specs[2] + "\t" + specs[3] + "\t" + specs[4] + "\t" +
                        specs[5] + "\t" + specs[6] + "\t" + specs[7] + "\t" + "1");
                multipleOutputs.write("SpecOrder", NullWritable.get(), outOrderValue);
                outCancelValue.set(cancels[1] + "\t" + "NULL" + "\t" + cancels[3] + "\t" + "NULL" + "\t" +
                        "U" + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            } else {
                outCancelValue.set(cancels[1] + "\t" + "NULL" + "\t" + cancels[3] + "\t" + "NULL" + "\t" +
                        "1" + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            }
            multipleOutputs.write("Cancel", NullWritable.get(), outCancelValue);
        } else {
            if (IsLimit) {
                outOrderValue.set(limits[1] + "\t" + limits[2] + "\t" + limits[3] + "\t" + limits[4] + "\t" +
                        limits[5] + "\t" + limits[6] + "\t" + limits[7] + "\t" + "2");
                multipleOutputs.write("LimitOrder", NullWritable.get(), outOrderValue);
            } else if (IsSpec) {
                outOrderValue.set(specs[1] + "\t" + specs[2] + "\t" + specs[3] + "\t" + specs[4] + "\t" +
                        specs[5] + "\t" + specs[6] + "\t" + specs[7] + "\t" + "2");
                multipleOutputs.write("SpecOrder", NullWritable.get(), outOrderValue);
            }
        }
    }

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }
    /*
      The cleanup method of the Reducer is responsible for closing the MultipleOutputs object.
    */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
