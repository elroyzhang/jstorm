/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.jstorm.ui.core.api.command;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.TopologyInfo;

import com.tencent.jstorm.daemon.nimbus.NimbusServer;
import com.tencent.jstorm.daemon.nimbus.ServiceHandler;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class KillTopologyServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;
  private final static Logger LOG = LoggerFactory
      .getLogger(KillTopologyServlet.class);
  private static final String FORMAT_XML = "xml";
  private static final String FORMAT_PARAM = "format";
  private static final String ID_PARAM = "id";
  private static final String WAITTIME_PARAM = "waitsecs";

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    String format = request.getParameter(FORMAT_PARAM);
    if (null == format) {
      format = FORMAT_XML;
    }

    String topId = request.getParameter(ID_PARAM);
    if (topId == null) {
      throw new IOException("id should not be null!");
    }
    String waitTime = request.getParameter(WAITTIME_PARAM);
    if (waitTime == null || !NumberUtils.isNumber(waitTime)) {
      waitTime = "5";
    }
    NimbusServer server =
        (NimbusServer) getServletContext().getAttribute("nimbus.server");
    ServiceHandler sh = server.getServiceHandler();
    TopologyInfo tplg;
    try {
      tplg = sh.getTopologyInfo(topId);
      String topologyName = tplg.get_name();
      KillOptions options = new KillOptions();
      options.set_wait_secs(Integer.valueOf(waitTime));
      sh.killTopologyWithOpts(topologyName, options);
      LOG.info("Killing topology '{}' with wait time: {} secs", topologyName,
          waitTime);
      response.sendRedirect("/api/v1/topology/id=" + topId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
