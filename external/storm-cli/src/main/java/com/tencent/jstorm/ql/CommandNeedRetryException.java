package com.tencent.jstorm.ql;

public class CommandNeedRetryException extends Exception {

  private static final long serialVersionUID = 1L;

  public CommandNeedRetryException() {
    super();
  }

  public CommandNeedRetryException(String message) {
    super(message);
  }

  public CommandNeedRetryException(Throwable cause) {
    super(cause);
  }

  public CommandNeedRetryException(String message, Throwable cause) {
    super(message, cause);
  }
}
