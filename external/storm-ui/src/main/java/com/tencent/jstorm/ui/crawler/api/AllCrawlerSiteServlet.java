package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.crawler.CrawlerDBQuery;
import com.tencent.jstorm.ui.db.connection.ConnectionProxy;
import com.tencent.jstorm.utils.CoreUtil;

public class AllCrawlerSiteServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      CrawlerDBQuery dbQuery = (CrawlerDBQuery) new ConnectionProxy()
          .getInstance(new CrawlerDBQuery());
      List<Map<String, Object>> siteIdList = dbQuery.getAllSiteId();
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartArray();
      for (Map<String, Object> map : siteIdList) {
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          dumpGenerator.writeStringField(entry.getKey(),
              String.valueOf(entry.getValue()));
        }
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.flush();
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      out.close();
    }
  }
}
