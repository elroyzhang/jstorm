package com.tencent.jstorm.ui.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MysqlDBUtil {
  
  public static void closeResultSet(ResultSet rs) throws SQLException {
    if (rs != null) {
      rs.close();
    }
  }
  
  public static void closeStatement(Statement st) throws SQLException {
    if (st != null) {
      st.close();
    }
  }
}
