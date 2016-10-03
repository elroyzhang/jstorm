package com.tencent.jstorm.ui.crawler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;

import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.ui.db.PageResult;
import com.tencent.jstorm.ui.db.QueryUtil;

@SuppressWarnings("unchecked")
public class CrawlerDBQuery {

  public PageResult queryCrawlerContent(Map<String, String> parameterMap)
      throws Exception {
    String siteId = parameterMap.get(ApiCommon.SITE_ID);
    String pageIndex = parameterMap.get(ApiCommon.PAGE_INDEX);
    String pageSize = parameterMap.get(ApiCommon.PAGE_SIZE);
    if (Integer.valueOf(pageSize) > 100) {
      throw new TException("query pageSize must smaller than 100!");
    }
    String table = getContentTableBySiteId(siteId);
    if (StringUtils.isEmpty(table)) {
      throw new TException("can not get table by site.id : " + siteId);
    }
    List<String> params = new ArrayList<String>();
    StringBuffer sql =
        new StringBuffer("select * from " + table + " where 1 = 1 ");
    for (Map.Entry<String, String> entry : parameterMap.entrySet()) {
      if (ApiCommon.SITE_ID.equals(entry.getKey())
          || ApiCommon.PAGE_INDEX.equals(entry.getKey())
          || ApiCommon.PAGE_SIZE.equals(entry.getKey())) {
        continue;
      }
      String key = entry.getKey();
      String value = entry.getValue();
      if (isDateTimeCondition(key, siteId)) {
        sql.append("and ").append(key).append(">=?");
        params.add(entry.getValue());
      } else if (value.indexOf("%") > -1) {
        sql.append("and ").append(key).append("like ").append(value);
      } else {
        sql.append("and ").append(entry.getKey()).append("=?");
        params.add(entry.getValue());
      }
    }
    if (params.size() < 1) {
      throw new TException("query condition must be provide at least one!");
    }
    PageResult pageResult = QueryUtil.queryPageResult(sql.toString(), params,
        Integer.valueOf(pageIndex), Integer.valueOf(pageSize));
    return pageResult;
  }

  private String getContentTableBySiteId(String siteId) throws Exception {
    String sql =
        "select content_table_name from SITE_TO_TABLE where site_id = ?";
    List<String> params = new ArrayList<String>();
    params.add(siteId);
    return QueryUtil.querySingleValue(sql, params);
  }

  public List<Map<String, Object>> getAllSiteId() throws Exception {
    String sql = "select * from SITE_TO_TABLE where 1 = 1";
    List<String> sqlParams = new ArrayList<String>();
    return QueryUtil.queryForMapListBySql(sql, sqlParams);
  }

  public List<Map<String, Object>> getTableFieldsBySiteId(String siteId)
      throws Exception {
    String tableName = getContentTableBySiteId(siteId);
    String sql =
        "select COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where table_name = ?";
    List<String> sqlParams = new ArrayList<String>();
    sqlParams.add(tableName);
    return QueryUtil.queryForMapListBySql(sql, sqlParams);
  }

  private boolean isDateTimeCondition(String key, String siteId)
      throws Exception {
    List<Map<String, Object>> fields = getTableFieldsBySiteId(siteId);
    for (Map<String, Object> field : fields) {
      if (key.equals(field.get("column_name"))) {
        if ("timestamp".equals(field.get("data_type"))) {
          return true;
        }
        break;
      }
    }
    return false;
  }
}
