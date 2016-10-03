package com.tencent.jstorm.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.thrift.transport.TTransport;

import org.apache.storm.Config;
import org.apache.storm.utils.NimbusClient;

import com.tencent.jstorm.ql.session.SessionState;

public class CliSessionState extends SessionState {

  /**
   * -e option if any that the session has been invoked with.
   */
  public String execString;

  /**
   * -f option if any that the session has been invoked with.
   */
  public String fileName;

  /**
   * properties set from -hiveconf via cmdline.
   */
  public Properties cmdProperties = new Properties();

  /**
   * -i option if any that the session has been invoked with.
   */
  public List<String> initFiles = new ArrayList<String>();

  protected String host;
  protected int port;

  private boolean remoteMode;

  private TTransport transport;
  private NimbusClient client;

  public CliSessionState(Config conf) {
    super(conf);
    remoteMode = false;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public void close() {
    try {
      super.close();
      if (remoteMode) {
        // client.clean();
        transport.close();
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  public boolean isRemoteMode() {
    return remoteMode;
  }

}
