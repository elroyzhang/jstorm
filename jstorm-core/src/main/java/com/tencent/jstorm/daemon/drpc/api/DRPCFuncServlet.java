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
package com.tencent.jstorm.daemon.drpc.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.daemon.DrpcServer;
import org.apache.storm.security.auth.IHttpCredentialsPlugin;
import org.apache.storm.security.auth.ReqContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:lynd.denglin@gmail.com">denglin</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.drpc#webapp")
public class DRPCFuncServlet extends HttpServlet {

  private static final Logger LOG =
      LoggerFactory.getLogger(DRPCFuncServlet.class);
  private static final long serialVersionUID = 1L;
  // private static final String FUNC_PARAM = "func";
  // private static final String ARGS_PARAM = "args";
  private String funcName = "";
  private String args = "";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    LOG.debug("URL: " + request.getRequestURL());

    if (!isValidReq(request)) {
      throw new ServletException(
          "Invalid drpc url : " + request.getRequestURL());
    }
    analyzeUrl(request);

    IHttpCredentialsPlugin httpCredsHandler =
        (IHttpCredentialsPlugin) getServletContext()
            .getAttribute("http.creds.handler");
    if (httpCredsHandler != null) {
      httpCredsHandler.populateContext(ReqContext.context(), request);
    }

    // TODO
    try {
      DrpcServer drpcServer =
          (DrpcServer) getServletContext().getAttribute("handler");
      LOG.debug("Get http drpc request; function name : {}, args : {}",
          new Object[] { funcName, args });
      response.setCharacterEncoding("utf-8");
      response.setContentType("text/html;chatset=utf-8");
      response.getWriter().write(drpcServer.execute(funcName, args));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    LOG.debug("URL: " + request.getRequestURL());

    if (!isValidReq(request)) {
      throw new ServletException(
          "Invalid drpc url : " + request.getRequestURL());
    }
    analyzeUrl(request);

    // get post args
    ServletInputStream sin = request.getInputStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffers = new byte[1024];
    int length = 0;
    while ((length = sin.read(buffers)) > 0) {
      baos.write(buffers, 0, length);
    }
    args = new String(baos.toByteArray(), "UTF-8");
    baos.close();

    IHttpCredentialsPlugin httpCredsHandler =
        (IHttpCredentialsPlugin) getServletContext()
            .getAttribute("http.creds.handler");
    if (httpCredsHandler != null) {
      httpCredsHandler.populateContext(ReqContext.context(), request);
    }
    // TODO
    try {
      DrpcServer drpcServer =
          (DrpcServer) getServletContext().getAttribute("handler");
      LOG.debug("POST http drpc request; function name : {}, args : {}",
          new Object[] { funcName, args });
      response.setCharacterEncoding("UTF-8");
      response.setContentType("text/html;chatset=utf-8");
      response.getWriter().write(drpcServer.execute(funcName, args));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * valid the request URL
   * 
   * for POST request, the url must like : /drpc/func
   * 
   * for GET request, the URL musr like : /drpc/func/ or /drpc/func/args
   * 
   */
  private boolean isValidReq(HttpServletRequest request) {
    String reqUrl = request.getRequestURI();
    String methodName = request.getMethod();
    if (reqUrl.endsWith("/")) {
      reqUrl = reqUrl.substring(0, reqUrl.length());
    }
    int cnt = StringUtils.countMatches(reqUrl, "/");
    if (methodName.equalsIgnoreCase("GET")) {

      if (cnt < 2) {
        return false;
      }
    } else if (methodName.equalsIgnoreCase("POST")) {
      if (cnt < 2) {
        return false;
      }
    } else {
      return false;
    }
    return true;
  }

  /**
   * parse the function name & arguments from the request URL
   */
  private void analyzeUrl(HttpServletRequest request) {
    String reqUrl = request.getRequestURI();

    reqUrl = reqUrl.substring(1);
    int firstIndex = reqUrl.indexOf('/');
    if (-1 == firstIndex) {
      funcName = reqUrl;
      return;
    } else {
      funcName = reqUrl.substring(0, firstIndex);
      reqUrl = reqUrl.substring(firstIndex);
    }

    int nextIndex = reqUrl.indexOf('/');
    if (-1 == nextIndex) {
      args = reqUrl;
    } else {
      args = reqUrl.substring(nextIndex + 1, reqUrl.length());
    }
  }

}
