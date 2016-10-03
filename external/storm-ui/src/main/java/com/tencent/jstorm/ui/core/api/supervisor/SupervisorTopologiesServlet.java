package com.tencent.jstorm.ui.core.api.supervisor;

import java.io.IOException;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.thrift.TException;

import com.tencent.jstorm.ui.core.Core;
import com.tencent.jstorm.ui.core.api.ApiCommon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 获取这个supervisor上运行的topologyid,taskid, hostname, port等信息
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 10:14:27 AM Mar 22, 2016
 */
public class SupervisorTopologiesServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("text/javascript");
    String supervisorId = request.getParameter(ApiCommon.SUPERVISOR_ID);
    String isIncludeSys = request.getParameter(ApiCommon.SYS);
    if (isIncludeSys == null) {
      isIncludeSys = "false";
    }
    OutputStreamWriter out = new OutputStreamWriter(response.getOutputStream());
    try {
      Core.nonEmptyParameterCheck(ApiCommon.SUPERVISOR_ID, supervisorId);
      Core.supervisorTopologyDetail(supervisorId, Core.checkIncludeSys(isIncludeSys), out);
    } catch (Exception e) {
      Core.restApiResponseWrite(response,
          String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR),
          CoreUtil.stringifyError(e));
    }
    out.close();
  }
}
