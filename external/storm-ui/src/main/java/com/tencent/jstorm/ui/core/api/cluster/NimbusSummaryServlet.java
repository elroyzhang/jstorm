package com.tencent.jstorm.ui.core.api.cluster;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.utils.CoreUtil;

public class NimbusSummaryServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("text/javascript");
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.nimbusSummary(out);
    } catch (TException e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
    out.close();
  }
}
