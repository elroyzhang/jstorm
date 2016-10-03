package com.tencent.jstorm.ui.core.api.topology;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.ui.helpers.ComponentNode;
import com.tencent.jstorm.utils.CoreUtil;

public class TopologyDAGServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  /**
   * show topology map (GET) /api/v1/topology/map?topology_id=$topology_id
   * [stream_id:
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String topologyId = req.getParameter(ApiCommon.TOPOLOGY_ID);
    try {
      Core.nonEmptyParameterCheck(ApiCommon.TOPOLOGY_ID, topologyId);
      Map<String, Map<String, List<ComponentNode>>> topologyMap =
          Core.topologyDAG(topologyId);
      List<String> showStrList = calculateComponentListString(topologyMap);

      OutputStreamWriter out = new OutputStreamWriter(resp.getOutputStream());
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);
      dumpGenerator.writeStartObject();
      dumpGenerator.writeFieldName("dags");
      dumpGenerator.writeStartArray();
      for (String str : showStrList) {
        dumpGenerator.writeStartObject();
        String[] strArr= str.split("\\|");
        if (strArr.length < 2) {
          continue;
        }
        dumpGenerator.writeStringField("stream", strArr[0]);
        dumpGenerator.writeStringField("dag", strArr[1]);
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
   * eg:STREAM_ID_COUNT]|word(1),exclaim(1),sender(1)
   * 
   * @param topologyMap
   * @return
   */
  private List<String> calculateComponentListString(
      Map<String, Map<String, List<ComponentNode>>> topologyMap) {
    List<String> componentShowStrList = new ArrayList<String>();
    for (Map.Entry<String, Map<String, List<ComponentNode>>> entry : topologyMap
        .entrySet()) {
      String streamId = entry.getKey();
      StringBuffer nodeListBuff = new StringBuffer(streamId + "|");
      Map<String, List<ComponentNode>> outputList = entry.getValue();
      for (Map.Entry<String, List<ComponentNode>> output : outputList
          .entrySet()) {
        List<ComponentNode> nodeList = output.getValue();
        for (ComponentNode node : nodeList) { 
          String componentStr = node.getComponent() + ":" + node.getExecutorNum();
          int index = nodeListBuff.toString().indexOf(componentStr);
          if (index > -1) {// 深度优先结束，表示是另一分支
            // 去掉后缀","
            String componentShowStr =
                nodeListBuff.substring(0, nodeListBuff.length() - 1);
            if (!componentShowStrList.contains(componentShowStr)) {
              componentShowStrList.add(componentShowStr);
            }
            nodeListBuff = new StringBuffer(
                nodeListBuff.substring(0, index + componentStr.length() + 1));
          } else {
            nodeListBuff.append(componentStr).append(",");
          }
        }
        String componentShowStr =
            nodeListBuff.substring(0, nodeListBuff.length() - 1);
        if (!componentShowStrList.contains(componentShowStr)) {
          componentShowStrList.add(componentShowStr);
        }
      }
    }
    return componentShowStrList;
  }
}
