package com.tencent.jstorm.command;

import java.util.Map;

import org.apache.storm.command.CLI;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.TopologyBackpressure;
import org.apache.storm.utils.NimbusClient;

import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 9:36:49 AM Jul 26, 2016
 */
public class ShowBackpressure {
  private static final String MSG_FORMAT = "%-20s %-10s\n";

  private static void printUsage() {
    System.out.println("Usage:");
    System.out
        .println("    $STORM_HOME/bin/storm backpressure -n topologyName");
  }

  public static void main(String[] args) throws Exception {
    try {
      Map<String, Object> cl =
          CLI.opt("n", "name", null, CLI.AS_STRING).parse(args);
      final String topologyName = (String) cl.get("n");
      if (topologyName == null) {
        System.err.println("Please provide topologyName!");;
        printUsage();
        return;
      }
      NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
        @Override
        public void run(Nimbus.Client nimbus) throws Exception {
          TopologyBackpressure backpressure =
              nimbus.getTopologyBackpressure(topologyName);
          if (backpressure.get_componentToBackpressure_size() == 0) {
            System.out.println("no backpressure info!!!");
            return;
          }
          int totalCount = 0;

          System.out.printf(MSG_FORMAT, "component", "backpressure-count");
          System.out.println("---------------------------------------");
          for (Map.Entry<String, Integer> componentBackpressure : backpressure
              .get_componentToBackpressure().entrySet()) {
            int count = componentBackpressure.getValue();
            String component = componentBackpressure.getKey();
            System.out.printf(MSG_FORMAT, component, count);
            totalCount += count;
          }
          System.out.println("");
          System.out.printf(MSG_FORMAT, "total", totalCount);
        }
      });
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    }
  }
}