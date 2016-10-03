package com.tencent.jstorm.ql.exec;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.storm.DynamicClassLoader;

public class Utils {

  public static ClassLoader addToClassPath(ClassLoader cloader,
      String[] newPaths) throws Exception {
    URLClassLoader loader = (URLClassLoader) cloader;
    List<URL> curPath = Arrays.asList(loader.getURLs());
    ArrayList<URL> newPath = new ArrayList<URL>();

    for (URL onePath : curPath) {
      newPath.add(onePath);
    }
    curPath = newPath;

    for (String onestr : newPaths) {
      if (StringUtils.indexOf(onestr, "file://") == 0) {
        onestr = StringUtils.substring(onestr, 7);
      }

      URL oneurl = (new File(onestr)).toURL();
      if (!curPath.contains(oneurl)) {
        curPath.add(oneurl);
      }
    }
    // DynamicClassLoader.updateLoader(curPath.toArray(new URL[0]));

    DynamicClassLoader dcl =
        new DynamicClassLoader(curPath.toArray(new URL[0]), loader);
    return dcl;
  }

  /**
   * StreamPrinter.
   * 
   */
  public static class StreamPrinter extends Thread {
    InputStream is;
    String type;
    PrintStream os;

    public StreamPrinter(InputStream is, String type, PrintStream os) {
      this.is = is;
      this.type = type;
      this.os = os;
    }

    @Override
    public void run() {
      BufferedReader br = null;
      try {
        InputStreamReader isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        String line = null;
        if (type != null) {
          while ((line = br.readLine()) != null) {
            os.println(type + ">" + line);
          }
        } else {
          while ((line = br.readLine()) != null) {
            os.println(line);
          }
        }
        br.close();
        br = null;
      } catch (IOException ioe) {
        ioe.printStackTrace();
      } finally {
        IOUtils.closeStream(br);
      }
    }
  }
}
