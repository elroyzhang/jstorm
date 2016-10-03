package com.tencent.jstorm.ql.parse;

import org.apache.storm.topology.IComponent;
import org.apache.storm.tuple.Fields;

public class BoltDesc {
  private IComponent bolt;
  private Integer parallelismHint;
  private String groupingType;
  private String componentId;
  private Fields fields;

  public BoltDesc(IComponent bolt, Integer parallelismHint,
      String groupingType, String componentId) {
    this(bolt, parallelismHint, groupingType, componentId, null);
  }

  public BoltDesc(IComponent bolt, Integer parallelismHint,
      String groupingType, String componentId, Fields fields) {
    this.bolt = bolt;
    this.parallelismHint = parallelismHint;
    this.groupingType = groupingType;
    this.componentId = componentId;
    this.fields = fields;
  }

  public Integer getParallelismHint() {
    return parallelismHint;
  }

  public void setParallelismHint(Integer parallelismHint) {
    this.parallelismHint = parallelismHint;
  }

  public String getGroupingType() {
    return groupingType;
  }

  public void setGroupingType(String groupingType) {
    this.groupingType = groupingType;
  }

  public IComponent getBolt() {
    return bolt;
  }

  public void setBolt(IComponent bolt) {
    this.bolt = bolt;
  }

  public Fields getFields() {
    return fields;
  }

  public void setFields(Fields fields) {
    this.fields = fields;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }
}
