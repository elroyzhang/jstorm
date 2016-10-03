package com.tencent.jstorm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.thread.Callback;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.Utils;

@ClojureClass(className = "org.apache.storm.daemon.util#launch-process#fn")
public class WaitForLaunchProcess extends RunnableCallback {
  private static final Logger LOG =
      LoggerFactory.getLogger(WaitForLaunchProcess.class);
  private static final long serialVersionUID = 1L;
  private Process process = null;
  private String logPrefix = null;
  private Callback callBack = null;

  public WaitForLaunchProcess(Process process, String logPrefix, Callback cb) {
    this.process = process;
    this.logPrefix = logPrefix;
    this.callBack = cb;
  }

  @Override
  public void run() {
    if (this.logPrefix != null) {
      Utils.readAndLogStream(this.logPrefix, this.process.getInputStream());
    }
    if (callBack != null) {
      try {
        this.process.waitFor();
      } catch (InterruptedException e) {
        LOG.info(
            this.logPrefix != null ? this.logPrefix : "" + " interrupted.");
      }
      callBack.execute(this.process.exitValue());
    }
  }

  @Override
  public Object getResult() {
    return -1;
  }
}
