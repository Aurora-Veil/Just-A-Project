package driver;

import mapper.OrderTypeMapper;
import mapper.TradeTypeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.MarketReducer;

import java.io.IOException;

public class MarketDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MarketSupplement");

        job.setJarByClass(MarketDriver.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, OrderTypeMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TradeTypeMapper.class);

        job.setReducerClass(MarketReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
