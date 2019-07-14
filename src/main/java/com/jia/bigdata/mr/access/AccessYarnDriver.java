package com.jia.bigdata.mr.access;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import com.jia.bigdata.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class AccessYarnDriver extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        final int res = ToolRunner.run(new Configuration(), new AccessYarnDriver(), args);
        System.exit(res);
    }

    public int run(final String[] args) throws Exception {
        final Job job = JobFactory.create(AccessDriver.class, AccessMapper.class, AccessReducer.class, Text.class, Access.class, Text.class, Access.class);
        job.setPartitionerClass((Class) AccessPartitioner.class);
        job.setNumReduceTasks(1);
        final Path path = new Path(args[1]);
        final FileSystem fileSystem = path.getFileSystem(job.getConfiguration());
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path[]{new Path(args[0])});
        FileOutputFormat.setOutputPath(job, path);
        final boolean result = job.waitForCompletion(true);
        return result ? 1 : 0;
    }
}
