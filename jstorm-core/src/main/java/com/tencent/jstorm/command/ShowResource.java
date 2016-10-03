package com.tencent.jstorm.command;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.Config;
import org.apache.storm.command.CLI;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
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
public class ShowResource {
  private static final String MSG_FORMAT =
      "%-20s %-15s %-15s %-20s %-10s %-10s %-10s %-10s\n";
  private static final String COUNT_FORMAT = "%-20s %-15s\n";

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("    $STORM_HOME/bin/storm resource [-h hostname]");
  }

  public static void main(String[] args) throws Exception {
    try {
      Map<String, Object> cl =
          CLI.opt("h", "host", null, CLI.AS_STRING).parse(args);
      final String hostNameRegx = (String) cl.get("h");
      final Pattern p;
      if (hostNameRegx != null) {
        p = Pattern.compile(hostNameRegx);
      } else {
        p = null;
      }
      NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
        @Override
        public void run(Nimbus.Client nimbus) throws Exception {
          List<SupervisorSummary> supervisorSummarys =
              nimbus.getSupervisorSummarys();
          if (supervisorSummarys.size() == 0) {
            System.out.println("no supervisor info!!!");
            return;
          }
          int totalCount = 0;

          System.out.printf(MSG_FORMAT, "host", "uptime_secs", "num_workers",
              "num_used_workers", "total_cpu", "total_mem", "used_cpu",
              "used_mem");
          System.out.println(
              "------------------------------------------------------------------------------------------------------------------------");
          for (SupervisorSummary supervisorSummary : supervisorSummarys) {
            String host = supervisorSummary.get_host();
            if (hostNameRegx != null) {
              Matcher matcher = p.matcher(host);
              if (!matcher.matches()) {
                continue;
              }
            }

            int uptime_secs = supervisorSummary.get_uptime_secs();
            int num_workers = supervisorSummary.get_num_workers();
            int num_used_workers = supervisorSummary.get_num_used_workers();
            double total_cpu = supervisorSummary.get_total_resources()
                .get(Config.SUPERVISOR_CPU_CAPACITY);
            double total_mem = supervisorSummary.get_total_resources()
                .get(Config.SUPERVISOR_MEMORY_CAPACITY_MB);
            double used_cpu = supervisorSummary.get_used_cpu();
            double used_mem = supervisorSummary.get_used_mem();
            System.out.printf(MSG_FORMAT, host, uptime_secs, num_workers,
                num_used_workers, total_cpu, total_mem, used_cpu, used_mem);
            totalCount += 1;
          }
          System.out.println("");
          System.out.printf(COUNT_FORMAT, "total", totalCount);
        }
      });
    } catch (Exception e) {
      System.out.println(CoreUtil.stringifyError(e));
      printUsage();
    }
  }
}

