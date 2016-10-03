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
package com.tencent.jstorm.ui.core.api.cluster;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;

/**
 * returns cluster configuration
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 *
 */
@ClojureClass(className = "backtype.storm.ui.core#cluster-configuration")
public class ClusterConfigurationServlet extends HttpServlet {

  private static final long serialVersionUID = 1L;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    // Do the authorization
    // if (!HttpServer.hasAdministratorAccess(getServletContext(), request,
    // response)) {
    // return;
    // }

    response.setContentType("text/javascript");
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      String keys = request.getParameter("config_key");
      List keysList = null;
      if (StringUtils.isNotEmpty(keys)) {
        String[] keysArr = keys.split(",");
        keysList = Arrays.asList(keysArr);
      }
      Core.clusterConfiguration(out, keysList);
    } catch (TException bfe) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(bfe));
    }
    out.close();
  }
}