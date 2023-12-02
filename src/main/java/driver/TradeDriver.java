package driver;

import mapper.TradeMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import reducer.MultipleTradedReducer;

public class TradeDriver {

    public static void main(String[] args) throws Exception {
        // 创建配置对象和作业对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MultipleOutputsExample");

        // 设置主类
        job.setJarByClass(TradeDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(TradeMapper.class);
        job.setReducerClass(MultipleTradedReducer.class);

        // 设置输入输出格式类
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输出键值对类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入输出路径
        TextInputFormat.addInputPath(job, new Path("hdfs://your-hdfs-path/input/trade_data.txt"));
        TextOutputFormat.setOutputPath(job, new Path("hdfs://your-hdfs-path/output/trade_output"));

        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
