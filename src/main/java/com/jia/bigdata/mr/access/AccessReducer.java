package com.jia.bigdata.mr.access;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import java.util.stream.*;
import java.io.*;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class AccessReducer extends Reducer<Text, Access, Text, Access>
{
    protected void reduce(final Text key, final Iterable<Access> values, final Reducer.Context context) throws IOException, InterruptedException {
        context.write((Object)key, (Object)StreamSupport.stream(values.spliterator(), false).reduce(new Access(key.toString()), (first, sec) -> first.add(sec)));
    }
}
