package driver;

import mapper.OrderMapper;
import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducer.DoAllReducer;

import java.io.IOException;

/**
 * Driver class for a Hadoop MapReduce job that appears to process stock market data.
 * This driver class orchestrates the entire MapReduce job, defining input sources,
 * Filtering th original records and completing the required field in Market Order.
 **/
public class DoAll {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Create a Hadoop configuration
        Configuration conf = new Configuration();

        // Create a new MapReduce job instance with a name
        Job job = Job.getInstance(conf, "Do step1 and step2");

        job.setJarByClass(DoAll.class);

        // Configure multiple input sources with corresponding mapper classes
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TradeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, TradeMapper.class);

        // Set the reducer class
        job.setReducerClass(DoAllReducer.class);

        // Set the key and value classes for the mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set the key and value classes for the final output
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Set the output path for the final output
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        // Configure multiple named outputs using MultipleOutputs
        MultipleOutputs.addNamedOutput(job, "Cancel", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "LimitOrder", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "SpecOrder", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "MarketOrder", TextOutputFormat.class, NullWritable.class, Text.class);

        // Submit the job and exit with success (0) or failure (1) status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
