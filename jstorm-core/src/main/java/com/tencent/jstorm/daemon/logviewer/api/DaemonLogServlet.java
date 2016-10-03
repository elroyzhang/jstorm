package com.tencent.jstorm.daemon.logviewer.api;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
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
@ClojureClass(className = "backtype.storm.daemon.logviewer#daemonlog")
public class DaemonLogServlet extends LogviewerServlet {

  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(DaemonLogServlet.class);

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String startStr = req.getParameter("start");
    String lengthStr = req.getParameter("length");
    try {
      String file = URLDecoder.decode(
          req.getParameter("file") == null ? "" : req.getParameter("file"));
      if (StringUtils.isEmpty(file)) {
        throw new Exception("file name can't be null!!!");
      }
      String daemonLogRoot = getDaemonlogRoot();
      String user = LogViewerUtils.httpcredshandler.getUserName(req);
      long start = -1;
      if (StringUtils.isNotEmpty(startStr)) {
        start = LogViewerUtils.parseLongFromMap("start", startStr);
      }
      long length = -1;
      if (StringUtils.isNotEmpty(lengthStr)) {
        length = LogViewerUtils.parseLongFromMap("length", lengthStr);
      }
      String grep = req.getParameter("grep");
      String damonlogPageHtml = LogViewerUtils.daemonlogPage(file, start,
          length, grep, user, daemonLogRoot);
      if (LogViewerUtils.PAGE_NOT_FOUND.equals(damonlogPageHtml)) {
        resp.setStatus(404);
        resp.getWriter().write(damonlogPageHtml);
      } else {
        LogViewerUtils.logTemplate(req, resp, damonlogPageHtml, file, user);
      }
    } catch (Exception e) {
      String errMsg = CoreUtil.stringifyError(e);
      LOG.error(errMsg);
      LogViewerUtils.ringResponseFromException(resp, errMsg);
    }
  }
}
