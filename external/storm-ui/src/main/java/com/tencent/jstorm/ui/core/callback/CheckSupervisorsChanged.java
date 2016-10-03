package com.tencent.jstorm.ui.core.callback;

import java.util.Map;

import org.apache.storm.generated.SupervisorInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.UIServer;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * For logviewer in docker, supervisor log link.
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 10:27:14 AM Jun 8, 2016
 */
public class CheckSupervisorsChanged extends RunnableCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(CheckSupervisorsChanged.class);
  private static final long serialVersionUID = 1L;

  private UIServer uiserver;

  public CheckSupervisorsChanged(UIServer uiserver) {
    this.uiserver = uiserver;
  }

  @Override
  public void run() {
    Map<String, SupervisorInfo> supervisorInfos;
    try {
      supervisorInfos =
          NimbusUtils.allSupervisorInfo(uiserver.getStormClusterState());
      LOG.info("supervisors : {}", supervisorInfos);

      UIServer.resetSupervisorIdToFirstSlot(
          Core.supervisorIdToFirstSlot(supervisorInfos));
    } catch (Exception e) {
      LOG.error("Check Supervisors Changed error {}",
          CoreUtil.stringifyError(e));
    }

    establishSupervisorsCallback();
  }

  public void establishSupervisorsCallback() {
    uiserver.getStormClusterState().supervisors(this);
  }
}
