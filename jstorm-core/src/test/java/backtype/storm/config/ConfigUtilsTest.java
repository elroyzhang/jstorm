package backtype.storm.config;

import org.junit.Test;

import org.apache.storm.utils.ConfigUtils;

import junit.framework.Assert;

public class ConfigUtilsTest {

  @Test
  public void testGetIdFromBlobKey() throws Exception {
    // match
    Assert.assertEquals("aa", ConfigUtils.getIdFromBlobKey("aa-stormjar.jar"));
    Assert.assertEquals(".", ConfigUtils.getIdFromBlobKey(".-stormcode.ser"));
    Assert.assertEquals("", ConfigUtils.getIdFromBlobKey("-stormconf.ser"));
    Assert.assertEquals(".-aa",
        ConfigUtils.getIdFromBlobKey(".-aa-stormcode.ser"));

    // dismatch
    Assert.assertEquals(null, ConfigUtils.getIdFromBlobKey("stormjar.jar"));
    Assert.assertEquals(null, ConfigUtils.getIdFromBlobKey("tormcode.ser"));
  }

}
