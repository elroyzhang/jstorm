package com.tencent.jstorm.tube;

import java.io.Serializable;

import org.apache.storm.spout.MultiScheme;
import org.apache.storm.spout.RawMultiScheme;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public class TubeConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	public final BrokerHosts hosts;
	public final String topic;
	public final String clientId;

	public int fetchSizeBytes = 1024 * 1024;
	public int socketTimeoutMs = 10000;
	public int fetchMaxWait = 10000;
	public int bufferSizeBytes = 1024 * 1024;
	public MultiScheme scheme = new RawMultiScheme();
	public boolean forceFromStart = false;
	public long maxOffsetBehind = Long.MAX_VALUE;
	public boolean useStartOffsetTimeIfOffsetOutOfRange = true;
	public int metricsTimeBucketSizeInSecs = 60;

	public TubeConfig(BrokerHosts hosts, String topic) {
		this(hosts, topic, "");
	}

	public TubeConfig(BrokerHosts hosts, String topic, String clientId) {
		this.hosts = hosts;
		this.topic = topic;
		this.clientId = clientId;
	}

}
