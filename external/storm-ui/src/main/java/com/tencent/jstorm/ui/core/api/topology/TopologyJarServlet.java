package com.tencent.jstorm.ui.core.api.topology;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

public class TopologyJarServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private static final int MAX_JAR_SIZE = 1024 * 1024 * 20;

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      response.setContentType("text/plain;charset=utf-8");
      if (request.getContentLength() > MAX_JAR_SIZE) {
        Core.restApiResponseWrite(response,
            String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
            "The Jar File exceed 20M, can't upload!");
        return;
      }
      String jarType = (String) request.getParameter(ApiCommon.JAR_TYPE);
      String jarName = (String) request.getParameter(ApiCommon.JAR_NAME);
      String siteId = (String) request.getParameter(ApiCommon.SITE_ID);
      String paramFileName =
          (String) request.getParameter(ApiCommon.PARAM_FILE_NAME);
      InputStream inputStream = request.getInputStream();
      Core.uploadTopologyFile(jarType, jarName, paramFileName, siteId, request);
      Map<String, String> respContent = new HashMap<String, String>();
      if (StringUtils.isEmpty(paramFileName)) {
        respContent.put("jar-name", jarName);
      } else {
        respContent.put("param-file-name", paramFileName);
      }
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK), "success upload jar!",
          respContent);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String jarType = (String) request.getParameter(ApiCommon.JAR_TYPE);
    String siteId = (String) request.getParameter(ApiCommon.SITE_ID);
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.nonEmptyParameterCheck(ApiCommon.JAR_TYPE, jarType);
      File[] listFile = Core.listTopologyJar(jarType, siteId);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("jar-files");
      dumpGenerator.writeStartArray();
      for (File tmpFile : listFile) {
        if (tmpFile.isDirectory()) {
          continue;
        }
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("file-name", tmpFile.getName());
        dumpGenerator.writeStringField("upload-time",
            Core.transferLongToDate(tmpFile.lastModified()));
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      out.close();
    }
  }

  public void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String jarType = (String) request.getParameter(ApiCommon.JAR_TYPE);
    String jarName = (String) request.getParameter(ApiCommon.JAR_NAME);
    String siteId = (String) request.getParameter(ApiCommon.SITE_ID);
    try {
      Core.nonEmptyParameterCheck(ApiCommon.JAR_TYPE, jarType);
      Core.nonEmptyParameterCheck(ApiCommon.JAR_NAME, jarName);
      Core.deleteTopologyJar(jarType, jarName, siteId);
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK),
          "success delete jar file[" + jarName + "]");
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }
}
