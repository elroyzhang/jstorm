package com.tencent.jstorm.bank.bolt.rdbms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import com.tencent.jstorm.ql.session.SessionState;
import com.tencent.jstorm.utils.StringUtils;

public class RDBMSDumperBolt implements IBasicBolt {
  private static final long serialVersionUID = 1L;
  private static transient RDBMSConnector connector = new RDBMSConnector();
  private static transient Connection con = null;
  private static transient RDBMSCommunicator communicator = null;
  private String tableName = null;
  private String primaryKey = null;
  private String dBUrl = null;
  private String password = null;
  private String username = null;
  private List<String> columnNames = new ArrayList<String>();
  private List<String> columnTypes = new ArrayList<String>();
  private List<Object> fieldValues = new ArrayList<Object>();
  private backtype.storm.Config conf;

  private static void printUsage() {
    System.out.println("RDBMSDumperBolt Usage:");
    System.out.println("   :SET topology.name=rdbmsbolt_topology;\n"
        + "    SET topology.workers=3;\n" + "    SET rdbms.tablename=testdb;\n"
        + "    SET rdbms.tablename=testdb;\n"
        + "    SET rdbms.columnnames=\"userid:username:age\";\n"
        + "    SET rdbms.columntype=\"varchar (100):varchar (100):int\";\n"
        + "    SET rdbms.url=\"jdbc:mysql://localhost:3306/testDB\";\n"
        + "    SET rdbms.username=\"root\";\n"
        + "    SET rdbms.password=\"root\";\n"
        + "    REGISTER spout=SPOUT(\"storm.starter.spout.SampleSpout\", 5);\n"
        + "    REGISTER split=BOLT(RDBMSDumperBolt, 8).SHUFFLE(\"spout\");\n"
        + "    SUBMIT;");

  }

  public RDBMSDumperBolt() throws SQLException {
    super();
    this.conf = SessionState.get().getConf();
    this.primaryKey =
        Utils.parseString((String) conf.get("rdbms.primay_key"), "N/A");
    this.tableName = (String) conf.get("rdbms.tablename");
    if (tableName == null) {
      printUsage();
      throw new SQLException("rdbms.tablename is null.");
    }
    String cnsStr = (String) conf.get("rdbms.columnnames");
    if (cnsStr == null) {
      printUsage();
      throw new SQLException("rdbms.columnnames is null.");
    }
    this.columnNames = StringUtils.mkStrToList(cnsStr, ":");
    String ctsStr = (String) conf.get("rdbms.columntype");
    if (ctsStr == null) {
      printUsage();
      throw new SQLException("rdbms.columntype is null.");
    }
    this.columnTypes = StringUtils.mkStrToList(ctsStr, ":");
    this.dBUrl = (String) conf.get("rdbms.url");
    if (dBUrl == null) {
      printUsage();
      throw new SQLException("rdbms.url is null.");
    }
    this.username = (String) conf.get("rdbms.username");
    if (username == null) {
      printUsage();
      throw new SQLException("rdbms.username is null.");
    }
    this.password = (String) conf.get("rdbms.password");
    if (password == null) {
      printUsage();
      throw new SQLException("rdbms.password is null.");
    }

    try {
      con = connector.getConnection(dBUrl, username, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    communicator =
        new RDBMSCommunicator(con, primaryKey, tableName, columnNames,
            columnTypes);
  }

  public RDBMSDumperBolt(String primaryKey, String tableName,
      ArrayList<String> columnNames, ArrayList<String> columnTypes,
      String dBUrl, String username, String password) throws SQLException {
    super();
    this.primaryKey = primaryKey;
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
    this.dBUrl = dBUrl;
    this.username = username;
    this.password = password;

    try {
      con = connector.getConnection(dBUrl, username, password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    communicator =
        new RDBMSCommunicator(con, primaryKey, tableName, columnNames,
            columnTypes);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    fieldValues = new ArrayList<Object>();
    Object fieldValueObject;
    // add all the tuple values to a list
    for (int i = 0; i < columnNames.size(); i++) {
      fieldValueObject = input.getValue(i);
      fieldValues.add(fieldValueObject);
    }
    // list passed as an argument to the insertRow funtion
    try {
      communicator.insertRow(fieldValues);
    } catch (SQLException e) {
      System.out.println("Exception occurred in adding a row ");
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // TODO Auto-generated method stub

  }

  public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    // TODO Auto-generated method stub

  }

  @Override
  public void cleanup() {
    // TODO Auto-generated method stub

  }

}
