package org.apache.storm.nimbus;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NimbusLeaderLatchListenerImpl implements LeaderLatchListener {
  private Map conf;
  private CuratorFramework zk;
  private LeaderLatch leaderLatch;
  private String hostName;
  private static final Logger LOG =
      LoggerFactory.getLogger(NimbusLeaderLatchListenerImpl.class);

  public NimbusLeaderLatchListenerImpl(Map conf, CuratorFramework zk,
      LeaderLatch leaderLatch) {
    this.conf = conf;
    this.zk = zk;
    this.leaderLatch = leaderLatch;
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
    }
  }

  @Override
  public void isLeader() {
    LOG.info("{} gained leadership", hostName);
    String path = conf.get(Config.STORM_ZOOKEEPER_ROOT)
        + ClusterUtils.nimbusPath(leaderLatch.getId());
    try {
      byte[] serialized = zk.getData().forPath(path);
      NimbusSummary nimbusSummary =
          ClusterUtils.maybeDeserialize(serialized, NimbusSummary.class);
      nimbusSummary.set_isLeader(true);
      zk.setData().forPath(path, Utils.serialize(nimbusSummary));
    } catch (Exception e) {
      LOG.error("set nimbus leader Exception", e);
    }

  }

  @Override
  public void notLeader() {
    LOG.info("{} lost leadership.", hostName);
    String path = conf.get(Config.STORM_ZOOKEEPER_ROOT)
        + ClusterUtils.nimbusPath(leaderLatch.getId());
    try {
      byte[] serialized = zk.getData().forPath(path);
      NimbusSummary nimbusSummary =
          ClusterUtils.maybeDeserialize(serialized, NimbusSummary.class);
      nimbusSummary.set_isLeader(false);
      zk.setData().forPath(path, Utils.serialize(nimbusSummary));
    } catch (Exception e) {
      LOG.error("set nimbus leader Exception", e);
    }

  }

}
