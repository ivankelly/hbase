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
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDummyLogWriterSmokeTest {

  private static final Log LOG = LogFactory.getLog(TestDummyLogWriterSmokeTest.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.regionserver.hlog.writer.impl", "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogWriter");
  }

  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testSuccesfulLoadOfDummyLogWriterFromConfiguration() throws Exception {
    Class<? extends Writer> logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl", 
                                                          SequenceFileLogWriter.class, Writer.class);
    assertEquals("org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogWriter", logWriterClass.getName());
  }
  
  @Test(expected=IOException.class)
  public void testAppendCannotSucceedIfWriterIsNotInitialized() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.append(generateFakeLogEntry(1, Bytes.toBytes("row1"), 1));
  }
  
  @Test
  public void testAppendASingleEntryIsSuccesful() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.init(null, null, null);
    dummyWriter.append(generateFakeLogEntry(1, Bytes.toBytes("row1"), 1));
    assertEquals(1, dummyWriter.getLength());
  }
  
  @Test
  public void testAppend20EntriesIsSuccesful() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.init(null, null, null);
    for(int i = 0; i < 20; i++) {
      dummyWriter.append(generateFakeLogEntry(i, Bytes.toBytes("row" + i), 1));
    }
    assertEquals(20, dummyWriter.getLength());
  }
  
  @Test(expected=IOException.class)
  public void testAppendAfterClosingThrowsException() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.init(null, null, null);
    dummyWriter.append(generateFakeLogEntry(1, Bytes.toBytes("row1"), 1));
    dummyWriter.close();
    dummyWriter.append(generateFakeLogEntry(2, Bytes.toBytes("row2"), 1));
  }
  
  /** Utility methods **/
  
  private HLog.Writer getDummyWriterInstance() throws InstantiationException, IllegalAccessException {
    Class<? extends Writer> logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl", 
          SequenceFileLogWriter.class, Writer.class);
    return (HLog.Writer) logWriterClass.newInstance();
  }

  private HLog.Entry generateFakeLogEntry(long logSeqNumber, byte[] row, int colCount) {
    return new HLog.Entry(generateFakeLogKey(logSeqNumber), generateEditForRowWithConfigurable1ByteColumns(row, colCount));
  }
  
  private HLogKey generateFakeLogKey(long seqNo) {
    return new HLogKey(Bytes.toBytes("TestRegion"), Bytes.toBytes("TestTable"), seqNo, HConstants.LATEST_TIMESTAMP, 
          HConstants.DEFAULT_CLUSTER_ID);
  }
  
  private WALEdit generateEditForRowWithConfigurable1ByteColumns(byte[] row, int colCount) {
    WALEdit edit = new WALEdit();
    long timestamp = System.currentTimeMillis();
    for (int i = 0; i < colCount; i++) {
      edit.add(new KeyValue(row, Bytes.toBytes("column"),
        Bytes.toBytes(Integer.toString(i)),
        timestamp, new byte[] { (byte) ('0') }));
    }
    return edit;
  }
}
