package com.tencent.jstorm.ql.exec;

import java.io.IOException;

import org.apache.commons.logging.Log;

public class IOUtils {

  public static void closeStream(java.io.Closeable stream) {
    cleanup(null, stream);
  }

  public static void cleanup(Log log, java.io.Closeable... closeables) {
    for (java.io.Closeable c : closeables) {
      if (c != null) {
        try {
          c.close();
        } catch (IOException e) {
          if (log != null && log.isDebugEnabled()) {
            log.debug("Exception in closing " + c, e);
          }
        }
      }
    }
  }
}
