package com.tencent.jstorm.ql.processors;

import com.tencent.jstorm.ql.CommandNeedRetryException;
import com.tencent.jstorm.ql.parse.Context;

public interface CommandProcessor {
  public void init();

  public CommandProcessorResponse run(String command, Context context)
      throws CommandNeedRetryException, Exception;
}
