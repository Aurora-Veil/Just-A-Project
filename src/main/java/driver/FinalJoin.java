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


import java.io.IOException;

public class FinalJoin {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 创建配置对象和作业对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FinalJoin");

        // 设置主类
        job.setJarByClass(FinalJoin.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);


        // 设置输出键值对类
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextInputFormat.addInputPath(job, new Path(args[1]));
        TextInputFormat.addInputPath(job, new Path(args[2]));
        TextInputFormat.addInputPath(job, new Path(args[3]));

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[4]));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
