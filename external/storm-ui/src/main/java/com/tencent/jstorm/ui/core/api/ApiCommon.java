package com.tencent.jstorm.ui.core.api;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ApiCommon {
  public static final String FORMAT_JSON = "json";
  public static final String FORMAT_XML = "xml";
  public static final String FORMAT_PARAM = "format";
  public static final String TOPOLOGY_ID = "tid";
  public static final String COMPONENT_ID = "cid";
  public static final String SUPERVISOR_ID = "sid";
  public static final String TOPOLOGY_NAME = "tname";

  // Default value :all-time| Window duration for metrics in seconds
  public static final String WINDOW = "window";
  // Values 1 or 0. Default value 0
  // Controls including sys stats part of theresponse
  public static final String SYS = "sys";
  public static final String JAR_TYPE = "jar-type";
  public static final String JAR_PATH = "jar-path";
  public static final String JAR_NAME = "jar-name";
  public static final String PARAM_FILE_NAME = "param-file-name";
  public static final String MAIN_CLASS = "main-class";
  public static final String WAIT_SECONDS = "wait-seconds";
  public static final String ARGS = "args";
  public static final String STORM_OPTIONS = "storm-options";
  public static final String ARGS_SPLITOR = ",";
  public static final String REQ_ACTION = "action";
  public static final String DEBUG = "debug";
  public static final String SAMPLING_PERCENTAGE = "sampling-percentage";
  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String PROFILE_ACTION = "profile-action";
  public static final String TIMEOUT = "timeout";


  /******* crawler job submit parameter begin ******/
  public static final String NUM_WORKERS = "num-workers";
  public static final String NUM_FRONTIER = "num-frontier";
  public static final String NUM_FETCHER_PARSER = "num-fetcher-parser";
  public static final String CRAWLER_FILE = "crawler-file";

  /******* crawler job submit parameter end ******/

  /******* crawler job conf parameter begin ******/
  // basic parameter
  public static final String JOB_CLASS = "crawl.job.class";
  public static final String CHILDOPTS = "crawl.job.childopts";
  public static final String SITE_TYPE = "site.type";
  public static final String SITE_ID = "site.id";
  public static final String FILTER_LIST = "filter.regex.list";
  public static final String MATCH_LIST = "match.regex.list";
  public static final String SEEDS = "seeds";

  // potional paramater
  public static final String CRALWERS_NUMBER = "number.of.crawlers";
  public static final String SCHEDULE_FREQUENCE = "crawl.job.schedule.frequence";
  public static final String COOKIES_IGNORE = "cookies.ignore";
  public static final String COOKIES_LOAD_FILE = "cookies.load.file";
  public static final String COOKIES_DOMAIN = "cookies.domain";
  public static final String POLITENESS_DELAY_MAX = "politeness.delay.max";
  public static final String POLITENESS_DELAY_MIN = "politeness.delay.min";
  public static final String ROBOTS_IS_OBEY = "robots.is.obey";
  public static final String TDSPIDER_DEBUG = "tdspider.debug";
  /******* crawler job conf parameter end ******/
  
  /******* crawler job conf parameter begin ******/
  public static final String PAGE_INDEX = "pageIndex";
  public static final String PAGE_SIZE = "pageSize";
  /******* crawler job conf parameter end ******/

  public static final String RESULT_CODE = "result_code";
  public static final String RESULT_MSG = "result_msg";
  public static final String RESULT_CONTENT = "result_content";


}
