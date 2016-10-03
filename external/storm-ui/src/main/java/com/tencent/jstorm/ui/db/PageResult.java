package com.tencent.jstorm.ui.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PageResult {

  private int totalCounts = 0;
  private List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
  private int pageIndex;
  private int pageSize;
  
  public int getTotalCounts() {
    return totalCounts;
  }
  public void setTotalCounts(int totalCounts) {
    this.totalCounts = totalCounts;
  }
  public List<Map<String, Object>> getResultList() {
    return resultList;
  }
  public void setResultList( List<Map<String, Object>> resultList) {
    this.resultList = resultList;
  }
  public int getPageIndex() {
    return pageIndex;
  }
  public void setPageIndex(int pageIndex) {
    this.pageIndex = pageIndex;
  }
  public int getPageSize() {
    return pageSize;
  }
  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }
}
