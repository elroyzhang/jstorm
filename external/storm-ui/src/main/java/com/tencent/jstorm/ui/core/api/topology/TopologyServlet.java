package com.tencent.jstorm.ui.core.api.topology;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.storm.generated.TopologySummary;
import org.apache.storm.utils.Utils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 用于submit/kill/reblance topology的restApi
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 11:19:52 AM Mar 15, 2016
 */
public class TopologyServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  /**
   * list topology (GET) /api/v1/topology args=args1,args2,args3...
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    try {
      List<TopologySummary> topologies = CoreUtil.listTopologies();
      OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("topologies");
      dumpGenerator.writeStartArray();
      for (TopologySummary topologySummary : topologies) {
        dumpGenerator.writeStartObject();
        dumpGenerator.writeStringField("topology_name",
            topologySummary.get_name());
        dumpGenerator.writeStringField("status", topologySummary.get_status());
        dumpGenerator.writeStringField("num_tasks",
            String.valueOf(topologySummary.get_num_tasks()));
        dumpGenerator.writeStringField("num_workers",
            String.valueOf(topologySummary.get_num_workers()));
        dumpGenerator.writeStringField("uptime_secs",
            String.valueOf(topologySummary.get_uptime_secs()));
        dumpGenerator.writeEndObject();
      }
      dumpGenerator.writeEndArray();
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      out.close();
    } catch (Exception e) {
      Core.restApiResponseWrite(resp,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }

  }

  /**
   * submit topology (POST)
   * /api/v1/topology?jar_path=$path&main_class=$main_class&topology_name=$name&
   * args=args1,args2,args3...
   */
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String jarName = req.getParameter(ApiCommon.JAR_NAME);
    String mainClass = req.getParameter(ApiCommon.MAIN_CLASS);
    String args = req.getParameter(ApiCommon.ARGS);
    String stormOptions = req.getParameter(ApiCommon.STORM_OPTIONS);
    String jarType = (String) req.getParameter(ApiCommon.JAR_TYPE);

    String resultCode = String.valueOf(HttpServletResponse.SC_OK);
    String resultMsg = "success to submit topology" ;
    try {
      Core.nonEmptyParameterCheck(ApiCommon.JAR_NAME, jarName);
      Core.nonEmptyParameterCheck(ApiCommon.MAIN_CLASS, mainClass);
      File topologyJarDir = Core.getTopologyJarDir(jarType);
      String uploadJarLocation = topologyJarDir.getCanonicalPath()
          + Utils.FILE_PATH_SEPARATOR + jarName;
      Core.submitTopologyForRestApi(uploadJarLocation, mainClass, args, stormOptions);
    } catch (Exception e) {
      resultCode = String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resultMsg = "failed to submit topology :" + CoreUtil.stringifyError(e);
    } finally {
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    }
  }

  /**
   * kill topology (DELETE)
   * /api/v1/topology?topology_name=topology_name1,topology_name2&wait_seconds=
   * $wait_seconds
   */
  @Override
  protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyName = req.getParameter(ApiCommon.TOPOLOGY_NAME);
    int waitSeconds = req.getParameter(ApiCommon.WAIT_SECONDS) == null ? 5
        : Integer.valueOf(req.getParameter(ApiCommon.WAIT_SECONDS));

    String resultCode = String.valueOf(HttpServletResponse.SC_OK);
    String resultMsg = "success to kill topology :" + topologyName;
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_NAME, topologyName);
      CoreUtil.killTopology(topologyName, waitSeconds);
    } catch (Exception e) {
      resultCode = String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resultMsg = "failed to kill topology :" + CoreUtil.stringifyError(e);
    } finally {
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    }
  }

  /**
   * reblance topology (PUT)
   * /api/v1/topology?topology_name=$name&wait=$wait_seconds&num-workers=$num-
   * workers&executor=$executor
   */
  @Override
  protected void doPut(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyName = req.getParameter(ApiCommon.TOPOLOGY_NAME);
    String numWorkers = req.getParameter(ApiCommon.NUM_WORKERS);
    String executors = req.getParameter("executors");
    int waitSeconds = req.getParameter(ApiCommon.WAIT_SECONDS) == null ? 5
        : Integer.valueOf(req.getParameter(ApiCommon.WAIT_SECONDS));

    String resultCode = String.valueOf(HttpServletResponse.SC_OK);
    String resultMsg = "success to rebalance topology :" + topologyName;
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_NAME, topologyName);
      Core.positiveParameterCheck(ApiCommon.NUM_WORKERS, numWorkers);
      CoreUtil.rebalanceTopology(topologyName, waitSeconds,
          Integer.valueOf(numWorkers), executors);
    } catch (Exception e) {
      resultCode = String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
      resultMsg = "failed to rebalance topology :" + CoreUtil.stringifyError(e);
    } finally {
      Core.restApiResponseWrite(resp, resultCode, resultMsg);
    }
  }
}
