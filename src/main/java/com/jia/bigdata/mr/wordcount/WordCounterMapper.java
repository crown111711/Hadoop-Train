package com.jia.bigdata.mr.wordcount;

import com.jia.bigdata.ExceptionWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        Arrays.stream(value.toString().split("\\s+")).filter(Objects::nonNull).filter(StringUtils::isNotBlank).forEach(e -> ExceptionWrapper.run(() -> context.write((Object)new Text(e), (Object)new IntWritable(1))));
    }
}
