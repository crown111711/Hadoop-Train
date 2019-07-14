package com.jia.bigdata.mr.access;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class AccessPartitioner extends Partitioner<Text, Access>
{
    public int getPartition(final Text text, final Access access, final int numPartitions) {
        final String phone = text.toString();
        if (phone.startsWith("13")) {
            return 0;
        }
        if (phone.startsWith("15")) {
            return 1;
        }
        return 2;
    }
}
