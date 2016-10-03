package com.tencent.jstorm.daemon.logviewer.api;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.logviewer.LogViewerUtils;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 4:37:29 PM Jul 7, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.logviewer#log-routes")
public class LogRoutesServlet extends LogviewerServlet {

  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(LogRoutesServlet.class);

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      String logRoot = getLogRoot();
      String user = LogViewerUtils.httpcredshandler.getUserName(req);
      long start = req.getParameter("start") == null ? -1
          : Long.valueOf(req.getParameter("start"));
      long length = req.getParameter("length") == null ? -1
          : Long.valueOf(req.getParameter("length"));
      String file = URLDecoder.decode(
          req.getParameter("file") == null ? "" : req.getParameter("file"));
      if (StringUtils.isEmpty(file)) {
        throw new Exception("file name can't be null!!!");
      }
      String grep = req.getParameter("grep");
      String logPageHtml =
          LogViewerUtils.logPage(file, start, length, grep, user, logRoot);
      if (LogViewerUtils.PAGE_NOT_FOUND.equals(logPageHtml)) {
        resp.setStatus(404);
        resp.getWriter().write(logPageHtml);
      } else {
        LogViewerUtils.logTemplate(req, resp, logPageHtml, file, user);
      }
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      LogViewerUtils.ringResponseFromException(resp, errMsg);
    }
  }
}
