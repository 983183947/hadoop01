package com.zzj.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: 小郑
 * Date: 2017-07-29
 * Time: 21:52
 *
 * KEYIN,VALUEIN,KEYOUT,VALUEOUT
 * KEYIN    map输出的key的类型
 * VALUEIN  map输出的value的类型
 * KEYOUT   我们Reducer要输出的数据的key的类型，String
 * VALUEOUT 我们Reducer辑要输出的数据的value的类型 Long
 *
 *
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    /**
     * 一组相同key的数据，调用一次reduce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        super.reduce(key, values, context);
            int count = 0;
            for(IntWritable value:values){
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
    }
}
