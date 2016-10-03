package com.tencent.jstorm.ui.core.api.drpc;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.crawler.CrawlerCore;
import com.tencent.jstorm.utils.CoreUtil;

public class DrpcResultServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String funcName = request.getParameter("func");
    String param = request.getParameter("param");
    try {
      Core.nonEmptyParameterCheck("func", funcName);
      String result = CrawlerCore.getCrawlerResultByDrpc(funcName, param);
      response.setCharacterEncoding("utf-8");
      response.setContentType("text/html;chatset=utf-8");
      response.getWriter().write(result);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    doGet(request, response);
  }
}