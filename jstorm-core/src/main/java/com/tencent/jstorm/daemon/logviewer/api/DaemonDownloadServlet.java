package com.tencent.jstorm.daemon.logviewer.api;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
 * @ModifiedTime 3:20:46 PM Jul 12, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.logviewer#daemondownload")
public class DaemonDownloadServlet extends LogviewerServlet {

  private static final long serialVersionUID = 1L;
  private static Logger LOG =
      LoggerFactory.getLogger(DaemonDownloadServlet.class);

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String file = URLDecoder.decode(
        req.getParameter("file") == null ? "" : req.getParameter("file"));
    String user = LogViewerUtils.httpcredshandler.getUserName(req);
    String daemonlogRoot = getDaemonlogRoot();
    try {
      LogViewerUtils.downloadLogFile(file, req, resp, user, daemonlogRoot);
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      LogViewerUtils.ringResponseFromException(resp, errMsg);
    }
  }
}
