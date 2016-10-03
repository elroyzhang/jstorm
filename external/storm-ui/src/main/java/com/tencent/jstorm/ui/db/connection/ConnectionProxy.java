package com.tencent.jstorm.ui.db.connection;

import java.lang.reflect.Method;
import java.sql.SQLException;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

public class ConnectionProxy implements MethodInterceptor {
  private Object target;

  public Object getInstance(Object target) {
    this.target = target;
    Enhancer enhancer = new Enhancer();
    enhancer.setSuperclass(this.target.getClass());
    enhancer.setCallback(this);
    return enhancer.create();
  }

  @Override
  public Object intercept(Object obj, Method method, Object[] args,
      MethodProxy proxy) throws Throwable {
    Object res = null;
    try {
      res = proxy.invokeSuper(obj, args);
      commit();
    } catch (Exception e) {
      e.printStackTrace();
      rollback();
      throw e;
    } finally {
      close();
    }
    return res;
  }

  private void rollback() throws SQLException {
    ConnectionContext.getContext().rollback();
  }

  private void commit() throws SQLException {
    ConnectionContext.getContext().commit();
  }

  private void close() throws SQLException {
    ConnectionContext.getContext().close();
  }
}
