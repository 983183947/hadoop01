package com.zzj.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 小郑
 * Date: 2017-07-29
 * Time: 23:06
 * job是 yarn的一个客户端，
 */
public class JobClient {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        conf.set("yarn.resourcemanager.hostname","192.168.0.144");
        Job job = Job.getInstance(conf,"testwordcount");

        job.setJarByClass(JobClient.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setNumReduceTasks(3);
        //告诉mapmaster，Mapper和reducer输出的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //告诉master要处理数据和输出结果所在目录
        //FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.0.144/"));
        //FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.0.144/"));
        FileInputFormat.setInputPaths(job, new Path("/zz01"));
        FileOutputFormat.setOutputPath(job,new Path("/wordcount1/output/"));
        //等到计算结果出来再结束进程，true打印到控制台
        boolean res = job.waitForCompletion(true);
        if(res){
            System.out.println("计算成功");
            System.exit(0);
        }else{
            System.out.println("计算失败");
            System.exit(1);
        }
    }
}
