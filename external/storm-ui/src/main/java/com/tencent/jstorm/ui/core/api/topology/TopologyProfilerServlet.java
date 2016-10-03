package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.ProfileAction;
import org.apache.storm.generated.ProfileRequest;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

public class TopologyProfilerServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topoId = req.getParameter(ApiCommon.TOPOLOGY_ID);
    String host = req.getParameter(ApiCommon.HOST);
    String port = req.getParameter(ApiCommon.PORT);
    String profileAction = req.getParameter(ApiCommon.PROFILE_ACTION);
    String timeout = req.getParameter(ApiCommon.TIMEOUT);

    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_ID, topoId);
      Core.nonEmptyParameterCheck(ApiCommon.HOST, host);
      Core.nonEmptyParameterCheck(ApiCommon.PROFILE_ACTION, profileAction);
      ProfileAction action =
          ProfileAction.findByValue(Integer.valueOf(profileAction));
      if (action == null) {
        throw new Exception(
            "can not find profile action by value :" + profileAction
                + ", it must be value of " + ProfileAction.values().toString());
      }
      NodeInfo nodeInfo = new NodeInfo();
      nodeInfo.set_node(host);
      Set<Long> ports = new HashSet<Long>();
      ports.add(Long.valueOf(port));
      nodeInfo.set_port(ports);
      ProfileRequest profileRequest = new ProfileRequest();
      profileRequest.set_nodeInfo(nodeInfo);
      profileRequest.set_action(action);
      Long timestamp = System.currentTimeMillis();
      if (action == ProfileAction.JPROFILE_START && timeout != null) {
        timestamp += (60000 * Integer.valueOf(timeout));
      }
      profileRequest.set_time_stamp(timestamp);

      Core.setWorkerProfiler(topoId, profileRequest);

      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      StringBuffer resultMsg = new StringBuffer();
      resultMsg.append("success set profile action").append(",topologyId ")
          .append(topoId).append(",host ").append(host)
          .append(",profileAction ").append(action).append(",port ")
          .append(port);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeStringField(ApiCommon.RESULT_CODE,
          String.valueOf(HttpServletResponse.SC_OK));
      dumpGenerator.writeStringField(ApiCommon.RESULT_MSG,
          resultMsg.toString());
      if (action == ProfileAction.JPROFILE_START) {
        dumpGenerator.writeStringField("timeout", timeout);
        dumpGenerator.writeStringField("timestamp",
            Core.transferLongToDate(timestamp));
        dumpGenerator.writeStringField("dumplink",
            Core.workerDumpLink(host, Integer.valueOf(port), topoId));
      }
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } catch (Exception e) {
      String resultCode =
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      String resultMsg =
          "failed to set profile action :" + CoreUtil.stringifyError(e);
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    } finally {
      out.close();
    }
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doPost(req, resp);
  }
}
