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

public class DoAll {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Do step1 and step2");

        job.setJarByClass(DoAll.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, OrderMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, TradeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, TradeMapper.class);

        job.setReducerClass(DoAllReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        MultipleOutputs.addNamedOutput(job, "Cancel", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "LimitOrder", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "SpecOrder", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "MarketOrder", TextOutputFormat.class, NullWritable.class, Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
