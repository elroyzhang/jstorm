package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.crawler.CrawlerCore;
import com.tencent.jstorm.utils.CoreUtil;

public class CrawlerResultServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String funcName = request.getParameter("func");
    String url = request.getParameter("url");
    String siteid = request.getParameter("siteid");
    try {
      Core.nonEmptyParameterCheck("url", url);
      Core.nonEmptyParameterCheck("siteid", siteid);
      String param = siteid + ":" + url;
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

}
