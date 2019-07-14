package com.jia.bigdata.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.stream.StreamSupport;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(final Text key, final Iterable<IntWritable> values, final Reducer.Context context) throws IOException, InterruptedException {
        final int ret = StreamSupport.stream(values.spliterator(), false).mapToInt(e -> e.get()).sum();
        context.write(key, new IntWritable(ret));
    }
}
