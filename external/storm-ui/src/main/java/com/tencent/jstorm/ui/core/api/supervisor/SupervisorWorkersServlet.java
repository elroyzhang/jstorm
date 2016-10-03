package com.tencent.jstorm.ui.core.api.supervisor;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
public class SupervisorWorkersServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;
  private static final String ID_PARAM = "host";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    // Do the authorization
    // if (!HttpServer.hasAdministratorAccess(getServletContext(), request,
    // response)) {
    // return;
    // }

    String hostName = request.getParameter(ID_PARAM);
    if (hostName == null) {
      throw new IOException("hostname should not be null!");
    }

    response.setContentType("text/javascript");
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.supervisorWorkers(hostName, out);
    } catch (Exception e) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    }
    out.close();
  }

}
