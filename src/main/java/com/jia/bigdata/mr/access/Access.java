package com.jia.bigdata.mr.access;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.*;

import java.io.*;
/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:09
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Access implements Writable {
    public String phone;
    public long up;
    public long down;
    public long sum;

    public Access(final String phone) {
        this.phone = phone;
    }

    public Access(final String phone, final long up, final long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public Access add(final Access access) {
        this.up += access.up;
        this.down += access.down;
        this.sum += access.sum;
        return this;
    }

    public void write(final DataOutput out) throws IOException {
        out.writeUTF(this.phone);
        out.writeLong(this.up);
        out.writeLong(this.down);
        out.writeLong(this.sum);
    }

    public void readFields(final DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }
}
