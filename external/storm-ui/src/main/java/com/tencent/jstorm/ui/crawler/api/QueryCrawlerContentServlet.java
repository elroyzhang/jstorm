package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.crawler.CrawlerDBQuery;
import com.tencent.jstorm.ui.db.PageResult;
import com.tencent.jstorm.ui.db.connection.ConnectionProxy;
import com.tencent.jstorm.utils.CoreUtil;

public class QueryCrawlerContentServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    Map<String, String> parameterMap = getParameterMap(request);
    try {
      CrawlerDBQuery dbQuery = (CrawlerDBQuery) new ConnectionProxy()
          .getInstance(new CrawlerDBQuery());
      PageResult crawlerContents = dbQuery.queryCrawlerContent(parameterMap);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeNumberField("totalCounts",
          crawlerContents.getTotalCounts());
      dumpGenerator.writeNumberField("pageSize", crawlerContents.getPageSize());
      dumpGenerator.writeNumberField("pageIndex",
          crawlerContents.getPageIndex());
      dumpGenerator.writeFieldName("result");
      dumpGenerator.writeStartArray();
      for (Map<String, Object> map : crawlerContents.getResultList()) {
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          dumpGenerator.writeStringField(entry.getKey(),
              String.valueOf(entry.getValue()));
        }
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

  @SuppressWarnings("unchecked")
  private Map<String, String> getParameterMap(HttpServletRequest request) {
    Map<String, String> jobConfMap = new HashMap<String, String>();
    Enumeration<String> parameterNames = request.getParameterNames();
    while (parameterNames.hasMoreElements()) {
      String key = (String) parameterNames.nextElement();
      String val = request.getParameter(key);
      jobConfMap.put(key, val);
    }
    return jobConfMap;
  }
}
