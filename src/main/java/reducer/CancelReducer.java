package reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class CancelReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MultipleOutputs<NullWritable, Text> multipleOutputs;
    private Text outOrderValue = new Text();
    private Text outCancelValue = new Text();

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean IsLimit = false;
        boolean IsSpec= false;
        boolean IsCancel = false;

        String Limit = "";
        String Spec = "";
        String Cancel = "";

        for (Text value: values) {
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
            }
        }

        String[] limits = Limit.split("\t");
        String[] specs = Spec.split("\t");

        if (IsCancel){
            String[] cancels = Cancel.split("\t");
            if (IsLimit){
                outOrderValue.set(limits[1] + "\t" + limits[2] + "\t" + limits[3] + "\t" + limits[4] + "\t" +
                        limits[5] + "\t" + limits[6] + "\t" + limits[7] + "\t" + "1");
                multipleOutputs.write("LimitOrder", NullWritable.get(), outOrderValue);
                outCancelValue.set(cancels[1] + "\t" + cancels[2] + "\t" + cancels[3] + "\t" + "2" + "\t" +
                        cancels[5] + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            } else if (IsSpec){
                outOrderValue.set(specs[1] + "\t" + specs[2] + "\t" + specs[3] + "\t" + specs[4] + "\t" +
                        specs[5] + "\t" + specs[6] + "\t" + specs[7] + "\t" + "1");
                multipleOutputs.write("SpecOrder", NullWritable.get(), outOrderValue);
                outCancelValue.set(cancels[1] + "\t" + cancels[2] + "\t" + cancels[3] + "\t" + "U" + "\t" +
                        cancels[5] + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            } else {
                outCancelValue.set(cancels[1] + "\t" + cancels[2] + "\t" + cancels[3] + "\t" + "1" + "\t" +
                        cancels[5] + "\t" + cancels[6] + "\t" + cancels[7] + "\t" + cancels[8]);
            }
            multipleOutputs.write("Cancel", NullWritable.get(), outCancelValue);
        } else{
            if (IsLimit){
                outOrderValue.set(limits[1] + "\t" + limits[2] + "\t" + limits[3] + "\t" + limits[4] + "\t" +
                        limits[5] + "\t" + limits[6] + "\t" + limits[7] + "\t" + "2");
                multipleOutputs.write("LimitOrder", NullWritable.get(), outOrderValue);
            } else if (IsSpec){
                outOrderValue.set(specs[1] + "\t" + specs[2] + "\t" + specs[3] + "\t" + specs[4] + "\t" +
                        specs[5] + "\t" + specs[6] + "\t" + specs[7] + "\t" + "2");
                multipleOutputs.write("SpecOrder", NullWritable.get(), outOrderValue);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}