package com.tencent.jstorm.ui.helpers;

public class ComponentNode {
  private String nodeType;
  private String component;
  private String streamId;
  private String prevComponent;
  private String outputField;
  private int executorNum;

  public ComponentNode(String nodeType, String component, String streamId, String inputField,
      String outputField, int executorNum) {
    this.nodeType = nodeType;
    this.component = component;
    this.streamId = streamId;
    this.prevComponent = inputField;
    this.outputField = outputField;
    this.executorNum = executorNum;
  }

  public String getNodeType() {
    return nodeType;
  }

  public void setNodeType(String nodeType) {
    this.nodeType = nodeType;
  }

  public String getComponent() {
    return component;
  }

  public void setComponent(String component) {
    this.component = component;
  }

  public String getStreamId() {
    return streamId;
  }

  public void setStreamId(String streamId) {
    this.streamId = streamId;
  }

  public String getPrevComponent() {
    return prevComponent;
  }

  public void setPrevComponent(String prevComponent) {
    this.prevComponent = prevComponent;
  }

  public String getOutputField() {
    return outputField;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public int getExecutorNum() {
    return executorNum;
  }

  public void setExecutorNum(int executorNum) {
    this.executorNum = executorNum;
  }
}
