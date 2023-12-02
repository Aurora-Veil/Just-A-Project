package driver;

import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducer.TradedReducer;

public class TradeDriver {

    public static void main(String[] args) throws Exception {
        // 创建配置对象和作业对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TradeSplit");

        // 设置主类
        job.setJarByClass(TradeDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(TradeMapper.class);
        job.setReducerClass(TradedReducer.class);

        // 设置输出键值对类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));

        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        MultipleOutputs.addNamedOutput(job, "Traded", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "Canceled", TextOutputFormat.class, Text.class, Text.class);

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
