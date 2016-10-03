package com.tencent.jstorm.ql.processors;

import java.util.HashSet;
import java.util.Set;

public enum StormCommand {
  SET(), RESET(), DFS(), ADD(), DELETE(), COMPILE();
  private static final Set<String> COMMANDS = new HashSet<String>();
  static {
    for (StormCommand command : StormCommand.values()) {
      COMMANDS.add(command.name());
    }
  }

  public static StormCommand find(String[] command) {
    if (null == command) {
      return null;
    }
    String cmd = command[0];
    if (cmd != null) {
      cmd = cmd.trim().toUpperCase();
      if (command.length > 1 && "role".equalsIgnoreCase(command[1])) {
        // special handling for set role r1 statement
        return null;
      } else if (COMMANDS.contains(cmd)) {
        return StormCommand.valueOf(cmd);
      }
    }
    return null;
  }

}
