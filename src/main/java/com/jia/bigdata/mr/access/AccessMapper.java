package com.jia.bigdata.mr.access;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

import java.io.*;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    protected void map(final LongWritable key, final Text value, final Mapper.Context context) throws IOException, InterruptedException {
        final String[] columns = value.toString().split("\t");
        final String phone = columns[1];
        final long up = Long.parseLong(columns[columns.length - 3]);
        final long down = Long.parseLong(columns[columns.length - 2]);
        context.write(new Text(phone), new Access(phone, up, down));
    }
}
