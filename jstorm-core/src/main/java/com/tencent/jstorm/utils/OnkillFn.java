package com.tencent.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.Utils;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 2:46:37 PM Feb 26, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon#onKillfn")
public class OnkillFn implements Thread.UncaughtExceptionHandler {
  private final static Logger LOG = LoggerFactory.getLogger(OnkillFn.class);

  @Override
  public void uncaughtException(Thread t, Throwable e) {
    LOG.error("Error when processing event {}", CoreUtil.stringifyError(e));
    Utils.exitProcess(20, "Error when processing an event");
  }

}
