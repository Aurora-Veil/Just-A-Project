package driver;

import mapper.JoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.JoinReducer;


import java.io.*;

/**
 * This class is the driver program for the final join MapReduce job. It combines data from multiple input sources
 * using a custom mapper and reducer to produce a consolidated output.
 */
public class FinalJoin {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Create a Hadoop configuration and a new MapReduce job instance with a name
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FinalJoin");

        // Set the main class
        job.setJarByClass(FinalJoin.class);

        // Set the Mapper and Reducer classes
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);


        // Set the output key and value classes for the Mapper
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Add multiple input paths using TextInputFormat
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextInputFormat.addInputPath(job, new Path(args[2]));
        TextInputFormat.addInputPath(job, new Path(args[3]));

        // Set the output path for the final output
        Path outputDir = new Path(args[4]);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Submit the job and exit with success (0) or failure (1) status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
