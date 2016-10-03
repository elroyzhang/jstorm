package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 用于activate/deactivate topology的restApi
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 11:23:36 AM Mar 15, 2016
 */
public class TopologyStatusServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * activate topology: (PUT)
   * /api/v1/topology/status?action=activate&topology_name=$name deactivate
   * topology: (PUT)
   * /api/v1/topology/status?action=deactivate&topology_name=$name
   */
  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String action = req.getParameter(ApiCommon.REQ_ACTION);
    String topologyName = req.getParameter(ApiCommon.TOPOLOGY_NAME);
    String resultCode = String.valueOf(HttpServletResponse.SC_OK);
    String resultMsg = "success to " + action + " topology :" + topologyName;
    try {
      Core.nonEmptyParameterCheck(ApiCommon.REQ_ACTION, action);
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_NAME, topologyName);
      if ("activate".equals(action)) {
        CoreUtil.activateTopology(topologyName);
      } else if ("deactivate".equals(action)) {
        CoreUtil.deactivateTopology(topologyName);
      } else {
        throw new Exception(action
            + "is not a valid action, please choose 'activate' or 'deactivate'");
      }
    } catch (Exception e) {
      resultCode = String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resultMsg =
          "failed to " + action + " topology :" + CoreUtil.stringifyError(e);
    } finally {
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    }
  }

}
