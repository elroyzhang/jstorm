package com.tencent.jstorm.cli;

import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

// A printStream that stores messages logged to it in a list.
public class CachingPrintStream extends PrintStream {

  List<String> output = new ArrayList<String>();

  public CachingPrintStream(OutputStream out, boolean autoFlush, String encoding)
      throws FileNotFoundException, UnsupportedEncodingException {

    super(out, autoFlush, encoding);
  }

  public CachingPrintStream(OutputStream out) {

    super(out);
  }

  @Override
  public void println(String out) {
    output.add(out);
    super.println(out);
  }

  @Override
  public void flush() {
    output = new ArrayList<String>();
    super.flush();
  }

  public List<String> getOutput() {
    return output;
  }
}
