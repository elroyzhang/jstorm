package com.tencent.jstorm.ql.processors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.tencent.jstorm.ql.CommandNeedRetryException;
import com.tencent.jstorm.ql.parse.Context;
import com.tencent.jstorm.ql.parse.VariableSubstitution;
import com.tencent.jstorm.ql.session.SessionState;
import com.tencent.jstorm.ql.session.SessionState.LogHelper;

public class AddResourceProcessor implements CommandProcessor {
  public static final Log LOG = LogFactory.getLog(AddResourceProcessor.class
      .getName());
  public static final LogHelper console = new LogHelper(LOG);

  @Override
  public void init() {

  }

  @Override
  public CommandProcessorResponse run(String command, Context context)
      throws CommandNeedRetryException {
    SessionState ss = SessionState.get();
    command = new VariableSubstitution().substitute(ss.getConf(), command);
    String[] tokens = command.split("\\s+");
    SessionState.ResourceType t;
    if (tokens.length < 2
        || (t = SessionState.find_resource_type(tokens[0])) == null) {
      console.printError("Usage: add ["
          + StringUtils.join(SessionState.ResourceType.values(), "|")
          + "] <value> [<value>]*");
      return new CommandProcessorResponse(1);
    }
    for (int i = 1; i < tokens.length; i++) {
      String resourceFile = ss.add_resource(t, tokens[i]);
      if (resourceFile == null) {
        String errMsg = tokens[i] + " does not exist.";
        return new CommandProcessorResponse(1, errMsg, null);
      }
    }

    return new CommandProcessorResponse(0);
  }

}
