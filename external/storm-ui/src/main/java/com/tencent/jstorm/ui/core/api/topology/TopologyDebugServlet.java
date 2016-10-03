package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

public class TopologyDebugServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyName = req.getParameter(ApiCommon.TOPOLOGY_NAME);
    String component = req.getParameter(ApiCommon.COMPONENT_ID);
    String debug = req.getParameter(ApiCommon.DEBUG);
    String samplingPercentage = req.getParameter(ApiCommon.SAMPLING_PERCENTAGE);

    String resultCode = String.valueOf(HttpServletResponse.SC_OK);
    String resultMsg = "success setting debug parameter,  topology[" + topologyName
        + "] component[" + component + "] debug[" + debug
        + "] sampling-percentage[" + samplingPercentage + "]";
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_NAME, topologyName);
      Core.nonEmptyParameterCheck(ApiCommon.DEBUG, debug);
      if (Boolean.valueOf(debug)) {
        Core.nonEmptyParameterCheck(ApiCommon.SAMPLING_PERCENTAGE,
            samplingPercentage);
      } else {
        samplingPercentage = "0";
      }
      Core.debug(topologyName, component, Boolean.valueOf(debug),
          Double.valueOf(samplingPercentage).doubleValue());
    } catch (Exception e) {
      resultCode = String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resultMsg = "failed to set debug parameter :" + CoreUtil.stringifyError(e);
    } finally {
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    }
  }
}
