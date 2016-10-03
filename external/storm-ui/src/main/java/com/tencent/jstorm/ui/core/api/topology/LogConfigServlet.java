package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jettison.json.JSONObject;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 4:21:25 PM Jun 12, 2016
 */
public class LogConfigServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  // (GET "/api/v1/topology/:id/logconfig" [:as {:keys [cookies
  // servlet-request]} id & m]
  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyId = req.getParameter(ApiCommon.TOPOLOGY_ID);
    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      Core.logConfig(topologyId, dumpGenerator);
    } catch (Exception e) {
      Core.restApiResponseWrite(resp,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      out.close();
    }
  }

  // (POST "/api/v1/topology/:id/logconfig" [:as {:keys [cookies
  // servlet-request]} id namedLoggerLevels & m]
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyId = req.getParameter(ApiCommon.TOPOLOGY_ID);
    InputStream is = req.getInputStream();
    OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_ID, topologyId);
      String contentStr = IOUtils.toString(is, "utf-8");
      JSONObject json = new JSONObject(contentStr);
      Map namedLoggerLevels = CoreUtil.jsonToMap(json);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      Core.logconfig(topologyId,
          (Map<String, Map<String, String>>) namedLoggerLevels
              .get("namedLoggerLevels"));
      Core.logConfig(topologyId, dumpGenerator);
    } catch (Exception e) {
      Core.restApiResponseWrite(resp,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      is.close();
      out.close();
    }
  }
}
