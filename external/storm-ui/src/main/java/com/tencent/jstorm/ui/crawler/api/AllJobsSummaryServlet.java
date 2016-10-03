package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.crawler.CrawlerCore;
import com.tencent.jstorm.utils.CoreUtil;

public class AllJobsSummaryServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      List<Map<String, String>> jobsSummary =  CrawlerCore.getAllJobsSummary();
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("job-summarys");
      dumpGenerator.writeStartArray();
      for (Map<String, String> job: jobsSummary) {
        dumpGenerator.writeStartObject();
        for (Map.Entry<String, String> entry : job.entrySet()) {
          dumpGenerator.writeStringField(entry.getKey(), entry.getValue());
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
}
