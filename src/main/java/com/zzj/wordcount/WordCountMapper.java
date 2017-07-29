package com.zzj.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 小郑
 * Date: 2017-07-29
 * Time: 21:52
 *
 *KEYIN,VALUEIN,KEYOUT,VALUEOUT
 * KEYIN  框架读取到的一行的数据起始偏移量，Long的值
 * VALUEIN  框架读取到的一行的内容，String
 * KEYOUT   我们业务逻辑要输出的数据的key的类型，String
 * VALUEOUT 我们业务逻辑要输出的数据的value的类型 Long
 *
 * 序列化，hadoop有自己的序列化方式
 *  Long --> LongWritable
 *  String -->Text
 *  Int --> IntWritable
 *  null-->NullWritable
 *
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    /**
     *
     * @param key  框架创给我们的KEYIN
     * @param value 框架传给我们的VALUEIN(拿到的这一行)
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       // super.map(key, value, context);
        String line=value.toString();
        String[] lineWords=line.split(" ");
        for(String word:lineWords){
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
