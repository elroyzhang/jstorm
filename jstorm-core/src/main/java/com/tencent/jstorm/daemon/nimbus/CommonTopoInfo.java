package com.tencent.jstorm.daemon.nimbus;

import java.util.Map;
import java.util.Set;

import org.apache.storm.cluster.IStormClusterState;

import com.tencent.jstorm.daemon.executor.ExecutorCache;

import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.StormTopology;

public class CommonTopoInfo {

  private IStormClusterState stormClusterState;
  @SuppressWarnings("rawtypes")
  private Map topologyConf;
  private String stormName;
  private StormTopology topology;
  private Map<Integer, String> taskToComponent;
  private StormBase base;
  private int launchTimeSecs;
  private Assignment assignment;
  private Map<ExecutorInfo, ExecutorCache> beats;
  private Set<String> allComponents;

  public IStormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public void setStormClusterState(IStormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  @SuppressWarnings("rawtypes")
  public Map getTopologyConf() {
    return topologyConf;
  }

  @SuppressWarnings("rawtypes")
  public void setTopologyConf(Map topologyConf) {
    this.topologyConf = topologyConf;
  }

  public String getStormName() {
    return stormName;
  }

  public void setStormName(String stormName) {
    this.stormName = stormName;
  }

  public StormTopology getTopology() {
    return topology;
  }

  public void setTopology(StormTopology topology) {
    this.topology = topology;
  }

  public Map<Integer, String> getTaskToComponent() {
    return taskToComponent;
  }

  public void setTaskToComponent(Map<Integer, String> taskToComponent) {
    this.taskToComponent = taskToComponent;
  }

  public StormBase getBase() {
    return base;
  }

  public void setBase(StormBase base) {
    this.base = base;
  }

  public int getLaunchTimeSecs() {
    return launchTimeSecs;
  }

  public void setLaunchTimeSecs(int launchTimeSecs) {
    this.launchTimeSecs = launchTimeSecs;
  }

  public Assignment getAssignment() {
    return assignment;
  }

  public void setAssignment(Assignment assignment) {
    this.assignment = assignment;
  }

  public Map<ExecutorInfo, ExecutorCache> getBeats() {
    return beats;
  }

  public void setBeats(Map<ExecutorInfo, ExecutorCache> beats) {
    this.beats = beats;
  }

  public Set<String> getAllComponents() {
    return allComponents;
  }

  public void setAllComponents(Set<String> allComponents) {
    this.allComponents = allComponents;
  }
}
