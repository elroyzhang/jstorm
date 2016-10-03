package com.tencent.jstorm.ui.core.api.supervisor;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;

/**
 * /api/v1/supervisor/conf?id=976e43e1-3e5d-422e-968c-994217801d9d
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
public class SupervisorConfServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  private static final String ID_PARAM = "id";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String supervisorId = request.getParameter(ID_PARAM);
    if (supervisorId == null) {
      throw new IOException("supervisorId should not be null!");
    }

    response.setContentType("text/javascript");

    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.supervisorConf(supervisorId, out);
    } catch (Exception bfe) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, bfe.getMessage());
    }
    out.close();
  }

  public static class BadFormatException extends Exception {
    private static final long serialVersionUID = 1L;

    public BadFormatException(String msg) {
      super(msg);
    }
  }
}
