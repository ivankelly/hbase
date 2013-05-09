package org.apache.hadoop.hbase.regionserver.wal.bk;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDummyLogReaderSmokeTest {

  private static final Log LOG = LogFactory.getLog(TestDummyLogReaderSmokeTest.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.regionserver.hlog.reader.impl", "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader");
  }

  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testSuccesfulLoadOfDummyLogReaderFromConfiguration() throws Exception {
    Class<? extends Reader> logWriterClass = conf.getClass("hbase.regionserver.hlog.reader.impl", 
                                                          SequenceFileLogReader.class, Reader.class);
    assertEquals("org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader", logWriterClass.getName());
  }
  
  /** Utility methods **/
  
  private HLog.Reader getDummyReaderInstance() throws InstantiationException, IllegalAccessException {
    Class<? extends Reader> logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", 
          SequenceFileLogReader.class, Reader.class);
    return (HLog.Reader) logReaderClass.newInstance();
  }

}
