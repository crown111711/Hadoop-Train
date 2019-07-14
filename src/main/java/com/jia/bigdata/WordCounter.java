package com.jia.bigdata;

import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */
public class WordCounter {
    public static void main(final String[] args) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://123.207.9.164:8020");
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.replication", "1");
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://123.207.9.164:8020"), configuration, "root");
        final Path path = new Path("/hdfsApi2/test/newtest3.txt");
        final RemoteIterator<LocatedFileStatus> iterator = (RemoteIterator<LocatedFileStatus>) fileSystem.listFiles(path, true);
        final Map<String, LongAdder> counter = Maps.newHashMap();
        while (iterator.hasNext()) {
            final LocatedFileStatus file = (LocatedFileStatus) iterator.next();
            final FSDataInputStream in = fileSystem.open(file.getPath());
            final BufferedReader reader = new BufferedReader(new InputStreamReader((InputStream) in));
            String line = "";
            while ((line = reader.readLine()) != null) {
                Arrays.stream(line.split("\\s+")).filter(StringUtils::isNotBlank).map(e -> e.replaceAll("\\s+", "")).forEach(e -> counter.computeIfAbsent(e, key -> new LongAdder()).add(1L));
            }
            reader.close();
            in.close();
        }
        final Path outPath = new Path("/hdfsApi2/test/");
        final FSDataOutputStream out = fileSystem.create(new Path(outPath, "worldCounter.out"));
        for (final Map.Entry<String, LongAdder> entry : counter.entrySet()) {
            out.writeUTF(entry.getKey().toString() + "\t" + entry.getValue().longValue() + "\n");
        }
    }
}
