package com.tencent.jstorm.disruptor;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.Credentials;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 11:26:56 AM Jan 18, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worke#mk-worker#check-credentials-changed")
public class CheckCredentialsChangedCallback extends RunnableCallback {

  private static final long serialVersionUID = 1L;
  private String stormId;
  private WorkerData worker;
  private AtomicReference<Credentials> credentials;
  private Collection<IAutoCredentials> autoCreds;
  private Subject subject;

  public CheckCredentialsChangedCallback(String stormId, WorkerData worker,
      AtomicReference<Credentials> credentials,
      Collection<IAutoCredentials> autoCreds, Subject subject) {
    this.stormId = stormId;
    this.worker = worker;
    this.credentials = credentials;
    this.autoCreds = autoCreds;
    this.subject = subject;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-worker#check-credentials-changed")
  public void run() {
    try {
      Credentials newCreds =
          worker.getStormClusterState().credentials(stormId, null);
      // This does not have to be atomic, worst case we update when one is not
      // needed
      if (newCreds != null && !newCreds.equals(credentials.get())) {
        AuthUtils.updateSubject(subject, autoCreds, newCreds.get_creds());
        for (ExecutorShutdown e : worker.getShutdownExecutors()) {
          e.crendentials_changed(newCreds);
        }
        credentials.set(newCreds);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
