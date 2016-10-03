package com.tencent.jstorm.ui.crawler.api;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.Config;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.ui.crawler.CrawlerCore;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * restapi for crawler job config
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 10:14:09 AM Apr 6, 2016
 */
public class JobConfigServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String siteId = request.getParameter(ApiCommon.SITE_ID);
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.nonEmptyParameterCheck(ApiCommon.SITE_ID, siteId);
      String jobFileString = CrawlerCore.getJobConfFileString(siteId);
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeString(jobFileString);
      dumpGenerator.flush();
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    } finally {
      out.close();
    }
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      String siteId = request.getParameter(ApiCommon.SITE_ID);
      Core.nonEmptyParameterCheck(ApiCommon.SITE_ID, siteId);
      validSiteId(siteId);
      String fileString = request.getParameter("jobFileString");
      // write job yaml file
      CrawlerCore.writeJobConfFile(siteId, fileString);
      Map<String, String> respCont = new HashMap<String, String>();
      respCont.put("site.id", siteId);
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK),
          "success config job file! site.id:" + siteId, respCont);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void doPut(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      String siteId = request.getParameter(ApiCommon.SITE_ID);
      String fileString = request.getParameter("jobFileString");
      Core.nonEmptyParameterCheck(ApiCommon.SITE_ID, siteId);
      CrawlerCore.updateJobConfFile(siteId, fileString);
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK),
          "success update job file!");
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void doDelete(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    try {
      String siteId = request.getParameter(ApiCommon.SITE_ID);
      Core.nonEmptyParameterCheck(ApiCommon.SITE_ID, siteId);
      // delete job yaml file
      CrawlerCore.deleteJobConfFile(siteId);
      Core.deleteTopologyJar("tdspider.jar", "", siteId);
      CrawlerCore.deleteSubmitCralwerFile(siteId);
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_OK),
          "success delete job file!");
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
  }

  private void validSiteId(String siteId) throws Exception {
    List<String> siteIds = CrawlerCore.getAllUserJobsId();
    for (String tmpSiteId : siteIds) {
      if (siteId.equals(tmpSiteId)) {
        throw new Exception("site.id[" + siteId
            + "] is exists, please provide another site.id, and the suggest site.id is "
            + (Integer.valueOf(siteIds.get(0)) + 1));
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> getParameterMap(HttpServletRequest request) {
    Map<String, String> jobConfMap = new HashMap<String, String>();
    Enumeration<String> parameterNames = request.getParameterNames();
    while (parameterNames.hasMoreElements()) {
      String key = (String) parameterNames.nextElement();
      String val = request.getParameter(key);
      jobConfMap.put(key, val);
    }
    return jobConfMap;
  }
}
