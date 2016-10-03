package com.tencent.jstorm.ui.crawler;

import java.util.List;

public class JobFile {
  /**basic element**/
  @JobElement(key = "crawl.job.class")
  private String jobClass;
  
  @JobElement(key = "crawl.job.childopts")
  private String childopts;
  
  @JobElement(key = "site.type", type = Integer.class)
  private String sitetype;
  
  @JobElement(key = "site.id", type = Integer.class)
  private String siteid;
  
  @JobElement(key = "filter.regex.list", type = List.class)
  private String filterList;
  
  @JobElement(key = "match.regex.list", type = List.class)
  private String matchList;
  
  @JobElement(key = "crawl.job.extractor.class", type = List.class)
  private String crawlJObExtractorClass;
  
  @JobElement(key = "seeds", type = List.class)
  private String seeds;

  /**optonal element**/
  @JobElement(key = "number.of.crawlers", type = Integer.class)
  private String crawlersNumber;
  
  @JobElement(key = "crawl.job.schedule.frequence", type = Integer.class)
  private String scheduleFrequence;
  
  @JobElement(key = "cookies.ignore", type = Boolean.class)
  private String cookiesIgnore;
  
  @JobElement(key = "cookies.load.file")
  private String cookiesLoadFile;
  
  @JobElement(key = "cookies.domain")
  private String cookiesDomain;
  
  @JobElement(key = "politeness.delay.max", type = Integer.class)
  private String politenessDelayMax;
  
  @JobElement(key = "politeness.delay.min", type = Integer.class)
  private String politenessDelayMin;
  
  @JobElement(key = "robots.is.obey", type = Boolean.class)
  private String robotsObey;
  
  @JobElement(key = "tdspider.debug", type = Boolean.class)
  private String tdspiderDebug;
  
  public String getJobClass() {
    return jobClass;
  }
  public void setJobClass(String jobClass) {
    this.jobClass = jobClass;
  }
  public String getChildopts() {
    return childopts;
  }
  public void setChildopts(String childopts) {
    this.childopts = childopts;
  }
  public String getSitetype() {
    return sitetype;
  }
  public void setSitetype(String sitetype) {
    this.sitetype = sitetype;
  }
  public String getSiteid() {
    return siteid;
  }
  public void setSiteid(String siteid) {
    this.siteid = siteid;
  }
  public String getFilterList() {
    return filterList;
  }
  public void setFilterList(String filterList) {
    this.filterList = filterList;
  }
  public String getMatchList() {
    return matchList;
  }
  public void setMatchList(String matchList) {
    this.matchList = matchList;
  }
  public String getSeeds() {
    return seeds;
  }
  public void setSeeds(String seeds) {
    this.seeds = seeds;
  }
  public String getCrawlersNumber() {
    return crawlersNumber;
  }
  public void setCrawlersNumber(String crawlersNumber) {
    this.crawlersNumber = crawlersNumber;
  }
  public String getScheduleFrequence() {
    return scheduleFrequence;
  }
  public void setScheduleFrequence(String scheduleFrequence) {
    this.scheduleFrequence = scheduleFrequence;
  }
  public String getCookiesIgnore() {
    return cookiesIgnore;
  }
  public void setCookiesIgnore(String cookiesIgnore) {
    this.cookiesIgnore = cookiesIgnore;
  }
  public String getCookiesLoadFile() {
    return cookiesLoadFile;
  }
  public void setCookiesLoadFile(String cookiesLoadFile) {
    this.cookiesLoadFile = cookiesLoadFile;
  }
  public String getCookiesDomain() {
    return cookiesDomain;
  }
  public void setCookiesDomain(String cookiesDomain) {
    this.cookiesDomain = cookiesDomain;
  }
  public String getPolitenessDelayMax() {
    return politenessDelayMax;
  }
  public void setPolitenessDelayMax(String politenessDelayMax) {
    this.politenessDelayMax = politenessDelayMax;
  }
  public String getPolitenessDelayMin() {
    return politenessDelayMin;
  }
  public void setPolitenessDelayMin(String politenessDelayMin) {
    this.politenessDelayMin = politenessDelayMin;
  }
  public String getRobotsObey() {
    return robotsObey;
  }
  public void setRobotsObey(String robotsObey) {
    this.robotsObey = robotsObey;
  }
  public String getTdspiderDebug() {
    return tdspiderDebug;
  }
  public void setTdspiderDebug(String tdspiderDebug) {
    this.tdspiderDebug = tdspiderDebug;
  }
  
}
