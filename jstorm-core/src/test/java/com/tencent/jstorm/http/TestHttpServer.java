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
package com.tencent.jstorm.http;

import java.io.IOException;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHttpServer extends HttpServerFunctionalTest {
  private static HttpServer server;
  private static URL baseUrl;
  private static final int MAX_THREADS = 10;

  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet(params.keySet());
      for (String key : keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for (int i = 1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }
  }

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      SortedSet<String> sortedKeys = new TreeSet();
      Enumeration<String> keys = request.getParameterNames();
      while (keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for (String key : sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    server = createTestServer();
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.start();
    baseUrl = getServerURL(server);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    server.stop();
  }

  /** Test the maximum number of threads cannot be exceeded. */
  @Test
  public void testMaxThreads() throws Exception {
    int clientThreads = MAX_THREADS * 10;
    Executor executor = Executors.newFixedThreadPool(clientThreads);
    // Run many clients to make server reach its maximum number of threads
    final CountDownLatch ready = new CountDownLatch(clientThreads);
    final CountDownLatch start = new CountDownLatch(1);
    for (int i = 0; i < clientThreads; i++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          ready.countDown();
          try {
            start.await();
            assertEquals("a:b\nc:d\n", readOutput(new URL(baseUrl,
                "/echo?a=b&c=d")));
            int serverThreads = server.webServer.getThreadPool().getThreads();
            assertTrue(serverThreads <= MAX_THREADS);
            System.out.println("Number of threads = " + serverThreads
                + " which is less or equal than the max = " + MAX_THREADS);
          } catch (Exception e) {
            // do nothing
          }
        }
      });
    }
    // Start the client threads when they are all ready
    ready.await();
    start.countDown();
  }

  @Test
  public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc<:d\ne:>\n", readOutput(new URL(baseUrl,
        "/echo?a=b&c<=d&e=>")));
  }

  /** Test the echo map servlet that uses getParameterMap. */
  @Test
  public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n", readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,>\nc<:d\n", readOutput(new URL(baseUrl,
        "/echomap?a=b&c<=d&a=>")));
  }

  public static class DummyServletFilter implements Filter {

    private static final Logger LOG = Logger
        .getLogger(DummyServletFilter.class);

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain filterChain) throws IOException, ServletException {
      final String userName = request.getParameter("user.name");
      ServletRequest requestModified =
          new HttpServletRequestWrapper((HttpServletRequest) request) {
            @Override
            public String getRemoteUser() {
              return userName;
            }
          };
      filterChain.doFilter(requestModified, response);
    }

    @Override
    public void init(FilterConfig arg0) throws ServletException {
    }
  }

  static int getHttpStatusCode(String urlstring, String userName)
      throws IOException {
    URL url = new URL(urlstring + "?user.name=" + userName);
    System.out.println("Accessing " + url + " as user " + userName);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    return connection.getResponseCode();
  }
}
