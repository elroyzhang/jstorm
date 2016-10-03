package com.tencent.jstorm.stats;

import org.apache.storm.utils.Utils;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.thread.BaseCallback;

/**
 * IncludeSysFn
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "backtype.storm.ui.core#mk-include-sys-fn")
public class IncludeSysFn extends BaseCallback {
  private boolean isIncludeSys;

  public IncludeSysFn(boolean isIncludeSys) {
    this.isIncludeSys = isIncludeSys;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Object execute(T... args) {
    if (isIncludeSys) {
      return true;
    }

    if (args != null && args.length > 0) {
      Object stream = (Object) args[0];
      return (stream instanceof String) && !Utils.isSystemId((String) stream);
    }

    return false;
  }

}
