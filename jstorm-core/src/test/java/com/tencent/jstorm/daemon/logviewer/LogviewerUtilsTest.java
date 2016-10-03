package com.tencent.jstorm.daemon.logviewer;

import org.junit.Test;

import junit.framework.Assert;

public class LogviewerUtilsTest {
  @Test
  public void testOffsetOfBytes() throws Exception {
    byte[] buf = new byte[] { 1, 2, 3, 4 };
    byte[] value = new byte[] { 2, 3 };
    Assert.assertEquals(1, LogViewerUtils.offsetOfBytes(buf, value, 0));
    Assert.assertEquals(1, LogViewerUtils.offsetOfBytes(buf, value, 1));
    Assert.assertEquals(-1, LogViewerUtils.offsetOfBytes(buf, value, 2));
    value = new byte[] { 1, 4 };
    Assert.assertEquals(-1, LogViewerUtils.offsetOfBytes(buf, value, 2));
  }

  public static void main(String[] args) {
    byte[] a = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
    System.arraycopy(a, 4, a, 0, 4);
    for (byte b : a) {
      System.out.println(b);
    }
    System.out.println(a.toString());
  }

}
