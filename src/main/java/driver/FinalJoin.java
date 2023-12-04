package driver;

import mapper.JoinMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.JoinReducer;


import java.io.*;

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
        Path outputDir = new Path(args[4]);
        FileOutputFormat.setOutputPath(job, outputDir);

        // 提交作业
        job.waitForCompletion(true);

        // 获取文件系统
        FileSystem fs = FileSystem.get(conf);

        // 获取原始输出文件路径
        Path originalOutputFile = new Path(args[4] +"/part-r-00000");

        // 新的输出文件路径
        Path finalOutputFile = new Path("Output/Output.txt");

        // 将原始输出文件改名为 Output.txt
        renameOutputFile(fs, originalOutputFile, finalOutputFile);
    }

    // 将输出文件改名
    private static void renameOutputFile(FileSystem fs, Path originalFile, Path finalFile) throws IOException {
        if (fs.rename(originalFile, finalFile)) {
            System.out.println("Output file renamed successfully.");
        } else {
            System.err.println("Failed to rename output file.");
        }
    }
}
