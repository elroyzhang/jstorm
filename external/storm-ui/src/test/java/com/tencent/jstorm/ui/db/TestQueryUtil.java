package com.tencent.jstorm.ui.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ui.crawler.CrawlerDBQuery;
import com.tencent.jstorm.ui.db.connection.ConnectionProxy;

public class TestQueryUtil {

  @Test
  public void testQueryPageResult() throws Exception {
    QueryUtil queryUtil = new QueryUtil();
    String sql =
        "select * from COM_51_JOB_JOB_INFO where education = ? order by digest";
    List<String> params = new ArrayList<String>();
    params.add("大专");
    PageResult jobList = queryUtil.queryPageResult(sql, params, 1, 10);
  }
  
  @Test
  public void testConnectionProxy() throws Exception {
    Map<String, String> parameterMap = new HashMap<String, String>();
    parameterMap.put("site.id", "67");
    parameterMap.put("pageIndex", "1");
    parameterMap.put("pageSize", "10");
    parameterMap.put("education", "大专");
    CrawlerDBQuery dbQuery = (CrawlerDBQuery) new ConnectionProxy()
        .getInstance(new CrawlerDBQuery());
    PageResult crawlerContents = dbQuery.queryCrawlerContent(parameterMap);
  }
  
}
