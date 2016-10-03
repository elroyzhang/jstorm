package com.tencent.jstorm.daemon.logviewer.api;

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.logviewer.LogViewerUtils;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 2:48:14 PM Jul 12, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.logviewer#/dumps?&tid=:topo-id&host-port=:host-port&fname=:filename")
public class DumpsServlet extends LogviewerServlet {

  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(DumpsServlet.class);

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String logRoot = getLogRoot();
    String topoId = req.getParameter("topo-id");
    String hostPort = req.getParameter("host-port");
    String filename = req.getParameter("fname");
    try {
      String user = LogViewerUtils.httpcredshandler.getUserName(req);
      String port = hostPort.split(":")[1];
      File dir = new File(logRoot + Utils.FILE_PATH_SEPARATOR + topoId
          + Utils.FILE_PATH_SEPARATOR + port + Utils.FILE_PATH_SEPARATOR
          + "dumps");
      if (StringUtils.isNotEmpty(filename)) {
        File file = new File(logRoot + Utils.FILE_PATH_SEPARATOR + topoId
            + Utils.FILE_PATH_SEPARATOR + port + Utils.FILE_PATH_SEPARATOR
            + "dumps" + Utils.FILE_PATH_SEPARATOR + filename);
        if (dir.exists() && file.exists()) {
          if (StringUtils
              .isEmpty((String) LogViewerUtils.STORM_CONF.get(Config.UI_FILTER))
              || LogViewerUtils.authorizedLogUser(user, filename,
                  LogViewerUtils.STORM_CONF)) {
            resp.setContentType("application/octet-stream");
            LogViewerUtils.responseFile(resp, file);
          } else {
            LogViewerUtils.unauthorizedUserHtml(user);
          }
        } else {
          resp.setStatus(404);
          resp.getWriter().write(LogViewerUtils.PAGE_NOT_FOUND);
        }
      } else {
        if (dir.exists()) {
          if (StringUtils
              .isEmpty((String) LogViewerUtils.STORM_CONF.get(Config.UI_FILTER))
              || LogViewerUtils.authorizedLogUser(user,
                  topoId + Utils.FILE_PATH_SEPARATOR + port
                      + Utils.FILE_PATH_SEPARATOR + "worker.log",
                  LogViewerUtils.STORM_CONF)) {

            resp.getWriter().println("<html>");

            // head
            resp.getWriter().println("<head>");
            resp.getWriter()
                .println("<title>File Dumps - Strorm Log Viewer</title>");
            resp.getWriter().println(
                "<link href='/css/bootstrap-3.3.1.min.css' rel='stylesheet' type='text/css'>");
            resp.getWriter().println(
                "<link href='/css/jquery.dataTables.1.10.4.min.css' rel='stylesheet' type='text/css'>");
            resp.getWriter().println(
                "<link href='/css/style.css' rel='stylesheet' type='text/css'>");
            resp.getWriter().println(
                "<script src='/js/jquery-1.11.1.min.js' type='text/javascript'></script>");
            resp.getWriter().println(
                "<script src='/js/logviewer.js' type='text/javascript'></script>");
            resp.getWriter().println("</head>");

            // body
            resp.getWriter().println("<body>");
            resp.getWriter().println("<ul>");
            for (File file : LogViewerUtils.getProfilerDumpFiles(dir)) {
              String href = "/dumps?topo-id=" + topoId + "&host-port="
                  + hostPort + "&fname=" + file.getName();
              resp.getWriter().println(
                  "<li><a href='" + href + "'>" + file.getName() + "</a></li>");
            }
            resp.getWriter().println("</ul>");
            resp.getWriter().println("</body>");
            resp.getWriter().println("</html>");
          }
        } else {
          resp.setStatus(404);
          resp.getWriter().write(LogViewerUtils.PAGE_NOT_FOUND);
        }
      }
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      resp.getWriter().write(errMsg);
    }
  }
}
