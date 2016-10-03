package com.tencent.jstorm.ql.processors;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.tencent.jstorm.ql.Driver;

public final class CommandProcessorFactory {

  private CommandProcessorFactory() {
    // prevent instantiation
  }

  private static final Map<Map, Driver> mapDrivers = Collections
      .synchronizedMap(new HashMap<Map, Driver>());

  public static CommandProcessor get(String cmd) throws SQLException {
    return get(new String[] { cmd }, null);
  }

  public static CommandProcessor getForStormCommand(String[] cmd, Map conf)
      throws SQLException {
    StormCommand stormCommand = StormCommand.find(cmd);
    if (stormCommand == null || isBlank(cmd[0])) {
      return null;
    }
    if (conf == null) {
      conf = new HashMap();
    }
    switch (stormCommand) {
    case SET:
      return new SetProcessor();
    case RESET:
      return new ResetProcessor();
    case ADD:
      return new AddResourceProcessor();
    case DELETE:
      return new DeleteResourceProcessor();
    case COMPILE:
      return new CompileProcessor();
    default:
      throw new AssertionError("Unknown StormCommand " + stormCommand);
    }
  }

  public static CommandProcessor get(String[] cmd, Map conf)
      throws SQLException {
    CommandProcessor result = getForStormCommand(cmd, conf);
    if (result != null) {
      return result;
    }
    if (isBlank(cmd[0])) {
      return null;
    } else {
      if (conf == null) {
        return new Driver();
      }
      Driver drv = mapDrivers.get(conf);
      if (drv == null) {
        drv = new Driver();
        mapDrivers.put(conf, drv);
      }
      drv.init();
      return drv;
    }
  }

  public static void clean(Map conf) {
    Driver drv = mapDrivers.get(conf);
    if (drv != null) {
      drv.destroy();
    }

    mapDrivers.remove(conf);
  }
}
