package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
@ClojureClass(className = "backtype.storm.ui.core#component-page")
public class ComponentPageServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private String topologyId;
  private String component;
  private String window;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    topologyId = request.getParameter(ApiCommon.TOPOLOGY_ID);
    if (topologyId == null) {
      throw new IOException("topology id should not be null!");
    }

    component = request.getParameter(ApiCommon.COMPONENT_ID);
    if (component == null) {
      throw new IOException("component id should not be null!");
    }

    window = request.getParameter(ApiCommon.WINDOW);
    if (window == null) {
      window = "0";
    }

    String isIncludeSys = request.getParameter(ApiCommon.SYS);//

    response.setContentType("text/javascript");
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());

    try {
      Core.componentPage(topologyId, component, window, isIncludeSys, out);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      if (null != out) {
        out.close();
      }
    }

  }

}
