package com.tencent.jstorm.metric;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.RuntimeMXBean;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.metric.api.IMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.MapKeyConstants;

public class ProcessMemoryUsageMetric implements IMetric {

  public static final Logger LOG =
      LoggerFactory.getLogger(ProcessMemoryUsageMetric.class);

  final static BigDecimal DIVISOR = BigDecimal.valueOf(1024);

  RuntimeMXBean jvmRT;

  public ProcessMemoryUsageMetric(RuntimeMXBean jvmRT) {
    this.jvmRT = jvmRT;
  }

  @Override
  public Object getValueAndReset() {
    return getProcessInfo();
  }

  private int getPid() {
    return Integer.parseInt(jvmRT.getName().split("@")[0]);
  }

  private Map<String, String> getProcessInfo() {
    Map<String, String> processInfo = new HashMap<String, String>();
    Runtime rt = Runtime.getRuntime();
    BufferedReader in = null;
    try {
      int pid = getPid();
      String[] cmd = { "/bin/sh", "-c", "top -b -n 1 -p " + pid };
      Process p = rt.exec(cmd);
      in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String str = null;
      String[] strArray = null;
      while ((str = in.readLine()) != null) {
        str = str.trim();
        strArray = str.split(" +");
        if (String.valueOf(pid).equals(strArray[0])) {
          LOG.debug(str);
          for (int i = 0; i < strArray.length; i++) {
            String info = strArray[i];
            if (info.trim().length() == 0) {
              continue;
            }
            if (i == 5) {// physical memory
              String unit = info.substring(info.length() - 1);
              if (unit.equalsIgnoreCase("g")) {
                processInfo.put(MapKeyConstants.PROCESS_PHYSICAL_MEMORY_USED,
                    info + " GB");
              } else if (unit.equalsIgnoreCase("m")) {
                BigDecimal memUseSize =
                    new BigDecimal(info.substring(0, info.length() - 1));
                processInfo
                    .put(MapKeyConstants.PROCESS_PHYSICAL_MEMORY_USED,
                        String.valueOf(memUseSize
                            .divide(DIVISOR, 2, BigDecimal.ROUND_HALF_UP)
                            .doubleValue()) + " GB");

              } else {
                BigDecimal memUseSize = new BigDecimal(info).divide(DIVISOR);
                processInfo
                    .put(MapKeyConstants.PROCESS_PHYSICAL_MEMORY_USED,
                        String.valueOf(memUseSize
                            .divide(DIVISOR, 2, BigDecimal.ROUND_HALF_UP)
                            .doubleValue()) + " GB");
              }
            }
            if (i == 8) {// cpu used
              processInfo.put(MapKeyConstants.PROCESS_CPU_USED, info + "%");
            }
            if (i == 9) {// memory used
              processInfo.put(MapKeyConstants.PROCESS_MEMORY_USED, info + "%");
            }
          }
          break;
        }
      }
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        LOG.error(CoreUtil.stringifyError(e));
      }
    }
    return processInfo;
  }
}