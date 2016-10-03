package com.tencent.jstorm.daemon.drpc;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.nimbus.NimbusInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;

public class DRPCServerInfo implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(DRPCServerInfo.class);
  private static final String DELIM = ":";

  private String host;
  private int port;
  private int invocationPort;
  private int httpPort;

  public DRPCServerInfo(String host, int port, int invocationPort,
      int httpPort) {
    this.host = host;
    this.port = port;
    this.invocationPort = invocationPort;
    this.httpPort = httpPort;
  }

  public static DRPCServerInfo parse(String drpcServerInfo) {
    String[] hostAndPorts = drpcServerInfo.split(DELIM);
    if(hostAndPorts != null && hostAndPorts.length == 4) {
        return new DRPCServerInfo(hostAndPorts[0], Integer.parseInt(hostAndPorts[1]),
            Integer.parseInt(hostAndPorts[2]),Integer.parseInt(hostAndPorts[3]));
    } else {
        throw new RuntimeException("DRPCServerInfo should have format of host:port:invocationPort:httpPort"
            + ", invalid string " + drpcServerInfo);
    }
  }

  @ClojureClass(className = "for docker")
  public static DRPCServerInfo fromConf(Map conf) {
    try {
      String host = InetAddress.getLocalHost().getCanonicalHostName();
      if (conf.containsKey(Config.STORM_LOCAL_HOSTNAME)) {
        host = conf.get(Config.STORM_LOCAL_HOSTNAME).toString();
        LOG.info("Overriding drpc server host to storm.local.hostname -> {}", host);
      }

      int port = Integer.parseInt(conf.get(Config.DRPC_PORT).toString());
      int invocationPort =
          Integer.parseInt(conf.get(Config.DRPC_INVOCATIONS_PORT).toString());
      int httpPort =
          Integer.parseInt(conf.get(Config.DRPC_HTTP_PORT).toString());
      Map<Integer, Integer> virualToRealPort =
          (Map<Integer, Integer>) conf.get("storm.virtual.real.ports");
      if (virualToRealPort != null && virualToRealPort.containsKey(port)) {
        LOG.info("Use {} ---> {}", port, virualToRealPort.get(port));
        port = virualToRealPort.get(port);
      }
      if (virualToRealPort != null
          && virualToRealPort.containsKey(invocationPort)) {
        LOG.info("Use {} ---> {}", invocationPort,
            virualToRealPort.get(invocationPort));
        invocationPort = virualToRealPort.get(invocationPort);
      }
      if (virualToRealPort != null && virualToRealPort.containsKey(httpPort)) {
        LOG.info("Use {} ---> {}", httpPort, virualToRealPort.get(httpPort));
        httpPort = virualToRealPort.get(httpPort);
      }
      return new DRPCServerInfo(host, port, invocationPort, httpPort);

    } catch (UnknownHostException e) {
      throw new RuntimeException(
          "Something wrong with network/dns config, host cant figure out its name",
          e);
    }
  }

  public String toHostPortString() {
    return String.format("%s%s%s%s%s%s%s", host, DELIM, port, DELIM,
        invocationPort, DELIM, httpPort);
  }

  public int getPort() {
    return port;
  }

  public int getInvocationPort() {
    return invocationPort;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public String getHost() {
    return host;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof DRPCServerInfo))
      return false;

    DRPCServerInfo that = (DRPCServerInfo) o;

    if (port != that.port)
      return false;
    if (invocationPort != that.invocationPort)
      return false;
    if (httpPort != that.httpPort)
      return false;
    return host.equals(that.host);
  }

  @Override
  public int hashCode() {
    int result = host.hashCode();
    result = 31 * result + port;
    result = 31 * result + invocationPort;
    result = 31 * result + httpPort;
    return result;
  }

  @Override
  public String toString() {
    return "DRPCServerInfo{" + "host='" + host + '\'' + ", port=" + port
        + ", invocationPort=" + invocationPort + ", httpPort=" + httpPort + '}';
  }
}
