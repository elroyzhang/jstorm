package com.tencent.jstorm.ui.core.callback;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ui.core.UIServer;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * Cache nimbuses host and thrift port in NimbusClient (for UIServer process
 * only), so NimbusClient don't need to connect zookeeper to fetch seeds
 * everytime.
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 10:27:14 AM Jun 8, 2016
 */
public class CheckNimbusesChanged extends RunnableCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(CheckNimbusesChanged.class);
  private static final long serialVersionUID = 1L;

  private UIServer uiserver;

  public CheckNimbusesChanged(UIServer uiserver) {
    this.uiserver = uiserver;
  }

  @Override
  public void run() {
    List<String> nimbuses =
        uiserver.getStormClusterState().nimbusHostports(null);
    if (nimbuses == null) {
      nimbuses = new ArrayList<String>();
    }
    LOG.info("nimbuses host->port : {}", nimbuses);

    NimbusClient.resetNimbusHostPorts(nimbuses);
    establishNimbusesCallback();
  }

  public void establishNimbusesCallback() {
    uiserver.getStormClusterState().nimbusHostports(this);
  }
}
