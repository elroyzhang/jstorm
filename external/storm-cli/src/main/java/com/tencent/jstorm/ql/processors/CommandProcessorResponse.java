package com.tencent.jstorm.ql.processors;

public class CommandProcessorResponse {
  private int responseCode;
  private String errorMessage;
  private String SQLState;

  public CommandProcessorResponse(int responseCode) {
    this(responseCode, null, null);
  }

  public CommandProcessorResponse(int responseCode, String errorMessage,
      String SQLState) {
    this.responseCode = responseCode;
    this.errorMessage = errorMessage;
    this.SQLState = SQLState;
  }

  public int getResponseCode() {
    return responseCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getSQLState() {
    return SQLState;
  }
}
