package com.tencent.storm.yarn.container;

import java.util.Map;

import com.yahoo.storm.yarn.StormAMRMClient;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
public interface IContainerCalculator {

  @SuppressWarnings("rawtypes")
  void prepare(StormAMRMClient client, Map conf);

  void schedule() throws Exception;
}
