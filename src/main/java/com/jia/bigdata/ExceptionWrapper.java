package com.jia.bigdata;

/**
 * @author tanjia
 * @email 378097217@qq.com
 * @date 2019/7/15 2:02
 */
public class ExceptionWrapper {
    public static final void run(final WrapperRunnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            sneakThrow(e);
        }
    }

    public static <T extends Throwable> void sneakThrow(final Throwable e) throws T {
        throw (T) e;
    }
}
