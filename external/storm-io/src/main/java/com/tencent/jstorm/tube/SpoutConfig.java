package com.tencent.jstorm.tube;

import java.io.Serializable;
import java.util.List;


/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class SpoutConfig extends TubeConfig implements Serializable {
    public List<String> zkServers = null;
    public Integer zkPort = null;
    public String zkRoot = null;
    public String id = null;
    public long stateUpdateIntervalMs = 2000;

    public SpoutConfig(BrokerHosts hosts, String topic, String zkRoot, String id) {
        super(hosts, topic);
        this.zkRoot = zkRoot;
        this.id = id;
    }
    
