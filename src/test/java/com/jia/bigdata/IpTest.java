package com.jia.bigdata;

import com.jia.bigdata.utils.IPParser;
import com.jia.bigdata.utils.IPSeeker;
import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.BufferedOutputStream;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/28 1:29
 * <p>
 * 1 国外Geoip服务 MaxMind：https://www.maxmind.com/en/geoip-demo
 * 2 国内Geoip服务 百度开放API: http://lbsyun.baidu.com/index.php?title=webapi/ip-api
 */
public class IpTest {
    @Test
    public void testIpParser() throws Exception {

        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("58.101.148.226");
        System.out.println(regionInfo.getCity());

        IOUtils.copyBytes(IPSeeker.class.getClassLoader().getResourceAsStream("qqwry.dat"),
                System.out, 1024 * 1024);


//        LookupService service = new LookupService(LookupService.class.getClassLoader()
//                .getResource("GeoLiteCity.dat").toURI().getPath(), LookupService.GEOIP_MEMORY_CACHE);
//        Location location = service.getLocation("58.101.148.226");
//        System.out.println(location.city);
    }
}
