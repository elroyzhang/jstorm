package com.tencent.jstorm.ui.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ui.db.connection.ConnectionContext;
import com.tencent.jstorm.utils.CoreUtil;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class QueryUtil {

  public static final FastDateFormat FAST_DATE_FORMAT =
      FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

  final static int MAX_SIZE = 2000;

  private static Logger LOG = LoggerFactory.getLogger(QueryUtil.class);

  public static PageResult queryPageResult(String sql, List<String> params,
      Integer pi, Integer ps) throws Exception {
    PageResult pageResult = new PageResult();
    String countSql = " select count(*) count from ( " + sql + " ) tmp";
    int totalCount = new Long(querySingleValue(countSql, params)).intValue();
    if (totalCount == 0) {
      return pageResult;
    }
    int startIndex = ps * (pi - 1);
    int endIndex = ps;
    String queryResultSql = sql + " limit " + startIndex + "," + endIndex;
    List resultList = queryForMapListBySql(queryResultSql, params);
    pageResult.setTotalCounts(totalCount);
    pageResult.setResultList(resultList);
    pageResult.setPageIndex(pi);
    pageResult.setPageSize(ps);
    return pageResult;
  }

  public static String querySingleValue(String sql, List<String> params)
      throws Exception {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    String returnValue = "";
    try {
      if (params == null) {
        params = new ArrayList();
      }
      conn = ConnectionContext.getContext().getConnection();
      stmt = conn.prepareStatement(sql);
      for (int i = 0, argsCnt = params.size(); i < argsCnt; i++) {
        stmt.setString(i + 1, (String) params.get(i));
      }
      rs = stmt.executeQuery();
      if (rs.next()) {
        returnValue = rs.getString(1);
      }
    } catch (Exception se) {
      LOG.error(CoreUtil.stringifyError(se));
      throw se;
    } finally {
      MysqlDBUtil.closeStatement(stmt);
      MysqlDBUtil.closeResultSet(rs);
    }
    return returnValue;
  }

  public static List queryForMapListBySql(String sql, List<String> sqlParams)
      throws Exception {
    List list = new ArrayList();
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = ConnectionContext.getContext().getConnection();
      stmt = conn.prepareStatement(sql);
      for (int i = 0; sqlParams != null && i < sqlParams.size(); i++) {
        stmt.setString(i + 1, sqlParams.get(i));
      }
      rs = stmt.executeQuery();
      list = handle(rs);
    } catch (SQLException se) {
      LOG.error(CoreUtil.stringifyError(se));
      throw se;
    } finally {
      MysqlDBUtil.closeStatement(stmt);
      MysqlDBUtil.closeResultSet(rs);
    }
    return list;
  }

  private static List handle(ResultSet rs) throws SQLException {
    List results = new ArrayList();
    int i = 0;
    while (rs.next() && i <= MAX_SIZE) {
      results.add(tranResultToMap(rs));
      i++;
    }
    return results;
  }

  private static HashMap tranResultToMap(ResultSet rs) throws SQLException {
    HashMap vo = new HashMap();
    int fieldcount = rs.getMetaData().getColumnCount();
    String columnName = "";
    String columnTypeName = "";
    for (int i = 0; i < fieldcount; i++) {
      columnTypeName = rs.getMetaData().getColumnTypeName(i + 1);
      columnName = rs.getMetaData().getColumnName(i + 1).toLowerCase();
      if ("timestamp".equals(columnTypeName.toLowerCase())) {
        String _dateVal = FAST_DATE_FORMAT.format(rs.getTimestamp(columnName));
        if (_dateVal != null && !_dateVal.equals("")) {
          if (_dateVal.indexOf(".") >= 0) {
            String[] _lstDateVal = _dateVal.split(".");
            vo.put(columnName, _lstDateVal[0]);
          } else {
            vo.put(columnName, _dateVal);
          }
        } else {
          vo.put(columnName, "");
        }
      } else if ("number".equals(columnTypeName.toLowerCase())) {
        Object colVal = rs.getObject(columnName);
        if (colVal == null) {
          colVal = "";
        }
        vo.put(columnName, String.valueOf(colVal));
      } else {
        Object colVal = rs.getObject(columnName);
        if (colVal == null) {
          colVal = "";
        }
        vo.put(columnName, colVal);
      }
    }
    return vo;
  }
}
