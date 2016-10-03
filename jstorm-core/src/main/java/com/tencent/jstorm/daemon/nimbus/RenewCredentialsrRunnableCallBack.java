package com.tencent.jstorm.daemon.nimbus;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.cluster.IStormClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.blobstore.BlobStore;
import org.apache.storm.generated.Credentials;
import org.apache.storm.security.auth.ICredentialsRenewer;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 11:15:28 AM Jan 7, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.nimbus#renew-credentials")
public class RenewCredentialsrRunnableCallBack implements Runnable {
  private final static Logger LOG =
      LoggerFactory.getLogger(RenewCredentialsrRunnableCallBack.class);
  private NimbusData nimbus;

  public RenewCredentialsrRunnableCallBack(NimbusData nimbus) {
    this.nimbus = nimbus;

  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void run() {
    try {
      if (NimbusUtils.isLeader(nimbus, false)) {
        IStormClusterState stormClusterState = nimbus.getStormClusterState();
        BlobStore blobStore = nimbus.getBlobStore();
        Collection<ICredentialsRenewer> renewers = nimbus.getCredRenewers();
        Object updateLock = nimbus.getCredUpdateLock();
        List<String> activeStorms = stormClusterState.activeStorms();
        if (activeStorms != null) {
          Set<String> assignedIds = new HashSet<String>(activeStorms);
          for (String id : assignedIds) {
            synchronized (updateLock) {
              Credentials origCredentials =
                  stormClusterState.credentials(id, null);
              Map topologyConf =
                  NimbusUtils.tryReadStormConf(nimbus.getConf(), id, blobStore);
              if (origCredentials != null) {
                Map<String, String> origCreds = origCredentials.get_creds();
                Map<String, String> newCreds =
                    new HashMap<String, String>(origCreds);
                for (ICredentialsRenewer renewer : renewers) {
                  LOG.info("Renewing Creds For " + id + " with " + renewer);
                  renewer.renew(newCreds,
                      Collections.unmodifiableMap(topologyConf));
                  if (!origCreds.equals(newCreds)) {
                    Credentials newCredentials = new Credentials(newCreds);
                    stormClusterState.setCredentials(id, newCredentials,
                        topologyConf);
                  }
                }

              }
            }
          }
        }
      } else {
        LOG.info("not a leader skipping , credential renweal.");
      }
    } catch (Exception e) {
      LOG.error("Renewing Creds exception ", e);
    }
  }
}
