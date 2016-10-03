
package com.tencent.jstorm.ui.db.connection;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ui.db.C3P0Util;
import com.tencent.jstorm.utils.CoreUtil;

@SuppressWarnings("rawtypes")
public class ConnectionContext {

  private static Logger LOG = LoggerFactory.getLogger(ConnectionContext.class);

  public static final String DEFAULT_DB_SOURCE = "mysql";

  private static class ConnectionContextThreadLocal extends ThreadLocal {
    protected Object initialValue() {
      return new ConnectionContext(null);
    }
  }

  private static ThreadLocal connectionContext =
      new ConnectionContextThreadLocal();

  private Connection currConn;

  public void setCurrConnection(Connection conn) {
    this.currConn = conn;
  }

  public Connection getCurrConnection() {
    return this.currConn;
  }

  public ConnectionContext(Connection conn) {
    this.currConn = conn;
  }

  public Connection getConnection() throws Exception {
    Connection conn = null;
    if (this.getCurrConnection() != null) {
      conn = this.getCurrConnection();
      try {
        if (conn != null && !conn.isClosed()) {
          return conn;
        }
      } catch (SQLException e) {
        LOG.error(CoreUtil.stringifyError(e));
        throw new Exception("connection is unvalid!");
      }
      this.setCurrConnection(null);
    }
    try {
      conn = getConnectionByC3P0();
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
    if (conn != null) {
      this.setCurrConnection(conn);
    } else {
      throw new Exception("can not get connection!");
    }
    return conn;
  }

  private Connection getConnectionByC3P0() throws SQLException {
    Connection conn = C3P0Util.getConnection();
    if (conn != null) {
      conn.setAutoCommit(false);
    }
    return conn;
  }

  public boolean commit() throws SQLException {
    Connection conn = this.getCurrConnection();
    if (conn == null) {
      return false;
    }
    if (!conn.isClosed()) {
      conn.commit();
    }
    return true;
  }

  public boolean close() throws SQLException {
    Connection conn = this.getCurrConnection();
    if (conn == null) {
      return false;
    }
    if (!conn.isClosed()) {
      conn.close();
    }
    return true;
  }

  public boolean rollback() throws SQLException {
    Connection conn = this.getCurrConnection();
    if (conn == null) {
      return false;
    }
    if (!conn.isClosed()) {
      conn.rollback();
    }
    return true;
  }

  public static ConnectionContext getContext() {
    ConnectionContext context = (ConnectionContext) connectionContext.get();
    if (context == null) {
      context = new ConnectionContext(null);
      setContext(context);
    }
    return context;
  }

  public static void setContext(ConnectionContext context) {
    connectionContext.set(context);
  }

  public void clearAll() {
    this.currConn = null;
  }

}
