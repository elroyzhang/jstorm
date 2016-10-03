package com.tencent.jstorm.daemon.logviewer.api;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.Config;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;

import com.tencent.jstorm.daemon.logviewer.LogViewerUtils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 5:00:23 PM Jul 7, 2016
 */
public class LogviewerServlet extends HttpServlet {

  private static Map conf = Utils.readStormConfig();
  private static String daemonlogRoot = LogViewerUtils
      .logRootDir((String) conf.get(Config.LOGVIEWER_APPENDER_NAME));
  private static String logRoot = ConfigUtils.workerArtifactsRoot(conf);

  public String getLogRoot() {
    return logRoot;
  }

  public void setLogRoot(String logRoot) {
    logRoot = logRoot;
  }

  public String getDaemonlogRoot() {
    return daemonlogRoot;
  }

  public void setDaemonlogRoot(String daemonlogRoot) {
    daemonlogRoot = daemonlogRoot;
  }

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    resp.getWriter().write(LogViewerUtils.PAGE_NOT_FOUND);
    resp.getWriter().flush();
    resp.getWriter().close();
  }
}
