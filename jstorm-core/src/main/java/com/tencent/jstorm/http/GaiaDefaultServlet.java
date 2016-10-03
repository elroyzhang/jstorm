package com.tencent.jstorm.http;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.resource.Resource;
import org.mortbay.util.URIUtil;

import com.tencent.jstorm.daemon.logviewer.LogViewer;

public class GaiaDefaultServlet extends DefaultServlet {

  private static final long serialVersionUID = 1L;

  @Override
  protected void sendDirectory(HttpServletRequest request,
      HttpServletResponse response, Resource resource, boolean parent)
          throws IOException {
    byte[] data = null;
    // /port_12345
    String prefix = "/" + LogViewer.getProxyPath();
    String base = URIUtil.addPaths(request.getRequestURI(), URIUtil.SLASH);
    if (!base.startsWith(prefix)) {
      base = prefix + base;
    }
    String dir = resource.getListHTML(base, parent);
    if (dir == null) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "No directory");
      return;
    }

    data = dir.getBytes("UTF-8");
    response.setContentType("text/html; charset=UTF-8");
    response.setContentLength(data.length);
    response.getOutputStream().write(data);

  }

}
