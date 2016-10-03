package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.ui.crawler.CrawlerCore;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * restapi for crawler job startup
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 10:15:09 AM Apr 6, 2016
 */
public class JobActionServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String siteId = request.getParameter(ApiCommon.SITE_ID);
    String jarName = request.getParameter(ApiCommon.JAR_NAME);
    String jarType = (String) request.getParameter(ApiCommon.JAR_TYPE);
    try {
//      Core.nonEmptyParameterCheck(ApiCommon.JAR_NAME, jarName);
      Core.nonEmptyParameterCheck(ApiCommon.SITE_ID, siteId);
      Core.nonEmptyParameterCheck(ApiCommon.JAR_TYPE, jarType);
      Map<String, String> submitMsgMap =
          CrawlerCore.submitCralwerJob(jarType, siteId, jarName);
      String errorMsg = submitMsgMap.get("errorMsg");
      String inputMsg = submitMsgMap.get("inputMsg");
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK),
          "submit job, execute msg: " + inputMsg + "error msg: " + errorMsg);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }
}
