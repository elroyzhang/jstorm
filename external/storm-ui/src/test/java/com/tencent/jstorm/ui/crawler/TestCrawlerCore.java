package com.tencent.jstorm.ui.crawler;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class TestCrawlerCore {
  @Test
  public void testWriteJobConfFile() throws Exception {
    Map<String, String> jobConfig = new HashMap<String, String>();
    jobConfig.put("crawl.job.class",
        "com.tencent.tdspider.rules.qq.NewsQQComRule");
    jobConfig.put("crawl.job.childopts", "-Xmx1024m");
    jobConfig.put("site.id", "999");
    jobConfig.put("site.type", "1");
    jobConfig.put("filter.regex.list",
        ".*(\\.(css|js|gif|jpg|png|mp3|mp3|zip|gz))$,.*coral.qq.com.*,.*v.qq.com.*,.*kf.qq.com.*,.*.t.qq.com.*,.*auto.qq.com.*,.*bbs\\.news\\.qq\\.com.*,.*nbadata\\.sports\\.qq\\.com.*,.*cbadata\\.sports\\.qq\\.com.*");
    jobConfig.put("match.regex.list", ".*.qq.com/a/\\d+/\\d+\\.htm.*");
    jobConfig.put("seeds",
        "http://news.qq.com/,http://view.news.qq.com/,http://tech.qq.com/,http://news.qq.com/society_index.shtml");
    jobConfig.put("number.of.crawlers", "1");
    jobConfig.put("crawl.job.schedule.frequence", "200");
    jobConfig.put("cookies.ignore", "true");
    jobConfig.put("cookies.load.file", "weibo_cookies.txt");
    jobConfig.put("politeness.delay.max", "1000");
    jobConfig.put("politeness.delay.min", "500");
    jobConfig.put("robots.is.obey", "true");
    jobConfig.put("tdspider.debug", "false");
    CrawlerCore.writeJobConfFile(jobConfig);
  }

  @Test
  public void testUpadateJobConfFile() throws Exception {
    Map<String, String> jobConfig = new HashMap<String, String>();
    jobConfig.put("site.id", "999");
    jobConfig.put("filter.regex.list",
        ".*(\\.(css|js|gif|jpg|png|mp3|mp3|zip|gz))$,.*coral.qq.com.*,");
    jobConfig.put("seeds", "http://news.qq.com/,http://view.news.qq.com/");
    jobConfig.put("number.of.crawlers", "11");
    jobConfig.put("crawl.job.schedule.frequence", "330");
    CrawlerCore.updateJobConfFile(jobConfig);
  }

  @Test
  public void testDeleteJobConfFile() throws Exception {
    CrawlerCore.deleteJobConfFile("999");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadJobConfFile() {
    HashMap<String, Object> jobConf =
        (HashMap<String, Object>) CrawlerCore.readYamlConfigFile(
            new File("/home/yuzhongliu/deploy/storm-current/jobs/999.yaml"));
    int frequence = jobConf.get("crawl.job.schedule.frequence") == null ? 3600
        : Integer.valueOf(
            String.valueOf(jobConf.get("crawl.job.schedule.frequence")));
    List<String> filterRegexList = jobConf.get("filter.regex.list") == null
        ? null : (List<String>) jobConf.get("filter.regex.list");
    List<String> matchRegexList = jobConf.get("match.regex.list") == null ? null
        : (List<String>) jobConf.get("match.regex.list");
    List<String> seeds = jobConf.get("seeds") == null ? null
        : (List<String>) jobConf.get("seeds");
  }

}
