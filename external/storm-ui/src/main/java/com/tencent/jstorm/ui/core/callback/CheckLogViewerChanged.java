package com.tencent.jstorm.ui.core.callback;

import java.util.HashMap;
import java.util.List;

import org.apache.storm.generated.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.UIServer;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * For logviewer in docker
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 10:27:14 AM Jun 8, 2016
 */
public class CheckLogViewerChanged extends RunnableCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(CheckLogViewerChanged.class);
  private static final long serialVersionUID = 1L;

  private UIServer uiserver;

  public CheckLogViewerChanged(UIServer uiserver) {
    this.uiserver = uiserver;
  }

  @Override
  public void run() {
    List<String> logviewerIds =
        uiserver.getStormClusterState().logviewers(null);
    HashMap<String, NodeInfo> realHostPorts = new HashMap<String, NodeInfo>();
    for (String logviewerId : logviewerIds) {
      realHostPorts.put(logviewerId,
          uiserver.getStormClusterState().logviewer(logviewerId));
    }

    LOG.info("logviewer nodes : {}", realHostPorts);

    UIServer.resetHostToRealPortsToLvPort(
        Core.hostToRPortsToLvRPort(realHostPorts));
    establishLogviewerCallback();
  }

  public void establishLogviewerCallback() {
    uiserver.getStormClusterState().logviewers(this);
  }
}
