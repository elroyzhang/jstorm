package com.tencent.jstorm.ui.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class C3P0Util {
  static ComboPooledDataSource cpds = null;

  static {
    // 这里有个优点，写好配置文件，想换数据库，简单
    // cpds = new ComboPooledDataSource("oracle");//这是oracle数据库
    cpds = new ComboPooledDataSource("mysql");// 这是mysql数据库
  }

  /**
   * 获得数据库连接
   * 
   * @return Connection
   * @throws SQLException
   */
  public static Connection getConnection() throws SQLException {
    return cpds.getConnection();
  }

  /**
   * 数据库关闭操作
   * 
   * @param conn
   * @param st
   * @param pst
   * @param rs
   */
  public static void close(Connection conn, PreparedStatement pst,
      ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {

      }
    }
    if (pst != null) {
      try {
        pst.close();
      } catch (SQLException e) {

      }
    }

    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
      }
    }
  }

  /**
   * 数据库关闭操作
   * 
   * @param conn
   * @param st
   * @param pst
   * @param rs
   */
  public static void close(Connection conn, PreparedStatement pst) {
    close(conn, pst, null);
  }

  /**
   * 数据库关闭操作
   * 
   * @param conn
   * @param st
   * @param pst
   * @param rs
   */
  public static void close(Connection conn) {
    close(conn, null, null);
  }
}