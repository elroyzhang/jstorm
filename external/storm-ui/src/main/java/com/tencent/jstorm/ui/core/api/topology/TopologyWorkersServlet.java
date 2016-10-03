package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "org.apache.storm.ui.core#topology-workers")
public class TopologyWorkersServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * get worker jvm info (GET) /api/v1/topology/workers?topology_id=$topology_id
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyId = req.getParameter(ApiCommon.TOPOLOGY_ID);
    String isIncludeSys = req.getParameter(ApiCommon.SYS);
    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      Core.topologyWorkers(topologyId, isIncludeSys, out);
    } catch (Exception e) {
      Core.restApiResponseWrite(resp,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      out.close();
    }
  }

}
