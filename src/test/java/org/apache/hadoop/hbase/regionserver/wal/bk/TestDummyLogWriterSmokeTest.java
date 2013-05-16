package org.apache.hadoop.hbase.regionserver.wal.bk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestDummyLogWriterSmokeTest {

  private static final Log LOG = LogFactory.getLog(TestDummyLogWriterSmokeTest.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("dfs.support.append", true);
    // The below config supported by 0.20-append and CDH3b2
    conf.setInt("dfs.client.block.recovery.retries", 2);
    TEST_UTIL.startMiniDFSCluster(3);
    Path hbaseRootDir = TEST_UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    conf.set(HConstants.HBASE_DIR, hbaseRootDir.toString());
  }

  @Before
  public void setUp() throws Exception {
    conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf.set(HConstants.HBASE_BK_WAL_ENABLED_KEY, "true");
    conf.set(HConstants.HBASE_BK_WAL_DUMMY_KEY, "true"); // use the dummy implementation
    conf.set("hbase.regionserver.hlog.writer.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogWriter");
    conf.set("hbase.regionserver.hlog.reader.impl",
            "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader");
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
    dummyWriter.init(null, URI.create("dummy://testURI"), null);
    dummyWriter.append(generateFakeLogEntry(1, Bytes.toBytes("row1"), 1));
    assertEquals(1, dummyWriter.getLength());
  }
  
  @Test
  public void testAppend20EntriesIsSuccesful() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.init(null, URI.create("dummy://testURI"), null);
    for(int i = 0; i < 20; i++) {
      dummyWriter.append(generateFakeLogEntry(i, Bytes.toBytes("row" + i), 1));
    }
    assertEquals(20, dummyWriter.getLength());
  }
  
  @Test(expected=IOException.class)
  public void testAppendAfterClosingThrowsException() throws Exception {
    HLog.Writer dummyWriter = getDummyWriterInstance();
    assert dummyWriter.getLength() == 0;
    dummyWriter.init(null, URI.create("dummy://testURI"), null);
    dummyWriter.append(generateFakeLogEntry(1, Bytes.toBytes("row1"), 1));
    dummyWriter.close();
    dummyWriter.append(generateFakeLogEntry(2, Bytes.toBytes("row2"), 1));
  }
  
  @Test
  public void testThatWhenCreating2RegionsEachOneOfThemCreatesADifferentLogFile() throws Exception {
    byte[] tableName = "testInitHRegionTable".getBytes();
    byte[] colFam = "DummyFamily".getBytes();
    byte[] col = "DummyCol".getBytes();
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(colFam));

    // We're going to put all foobar-1 to foobar-11 (excluded) rows in region 1 and...
    HRegionInfo hRegionInfo1 = new HRegionInfo(htd.getName(), Bytes.toBytes("foobar-1")/*startKey*/,
                                               Bytes.toBytes("foobar-11") /*stopKey*/,
                                               false, (long)new String(tableName).hashCode());
    // ...rows starting from foobar-11 in region 2
    HRegionInfo hRegionInfo2 = new HRegionInfo(htd.getName(), Bytes.toBytes("foobar-11")/*startKey*/,
                                               null /*stopKey*/,
                                               false, (long)new String(tableName).hashCode());
    HRegion hRegion1 = HRegion.createHRegion(hRegionInfo1, new Path(conf.get(HConstants.HBASE_DIR)), conf, htd);
    HRegion hRegion2 = HRegion.createHRegion(hRegionInfo2, new Path(conf.get(HConstants.HBASE_DIR)), conf, htd);
    long walStartSeqNoRegion1 = hRegion1.getLog().getSequenceNumber() + 1;
    long walStartSeqNoRegion2 = hRegion2.getLog().getSequenceNumber() + 1;
    
    // Write 2 records in each region: hRegion1 (foobar-1 to foobar-11) and hRegion2 (from foobar-11)
    Put p1r1 = new Put(Bytes.toBytes("foobar-1"));
    p1r1.add(colFam, col, Bytes.toBytes("Value-test"));
    hRegion1.put(p1r1);
    
    Put p2r1 = new Put(Bytes.toBytes("foobar-10"));
    p2r1.add(colFam, col, Bytes.toBytes("Value-test"));
    hRegion1.put(p2r1);

    Put p1r2 = new Put(Bytes.toBytes("foobaz-201"));
    p1r2.add(colFam, col, Bytes.toBytes("Value-test"));
    hRegion2.put(p1r2);
    
    Put p2r2 = new Put(Bytes.toBytes("foobaz-202"));
    p2r2.add(colFam, col, Bytes.toBytes("Value-test"));
    try { // Exercise a failure in writing to a wrong region
      hRegion1.put(p2r2);
      fail("Should not be here");
    } catch(WrongRegionException expected) {
      hRegion2.put(p2r2); // Then write to the right one
    }

    URI region1Uri = URI.create("dummy://"
                                + new String(hRegionInfo1.getTableName(), Charsets.UTF_8)
                                + "/" + hRegionInfo1.getEncodedName()
                                + "/" + walStartSeqNoRegion1);
    URI region2Uri = URI.create("dummy://"
                                + new String(hRegionInfo2.getTableName(), Charsets.UTF_8)
                                + "/" + hRegionInfo2.getEncodedName()
                                + "/" + walStartSeqNoRegion2);
    
    // Assert, per each region that the amount of stuff written by each 
    // DummyWriter is equal to what be have written above
    Writer region1Writer = DummyLogWriter.regionWriters.get(region1Uri);
    assertNotNull(region1Writer);
    assertEquals(2, region1Writer.getLength());
    Writer region2Writer = DummyLogWriter.regionWriters.get(region2Uri);
    assertNotNull(region2Writer);
    assertEquals(2, region2Writer.getLength());
    // Finally assert that the two files where log is stored are different for each region
    String msg = "Expected <" + region1Uri + "> to be unequal to <" + region2Uri +">";
    assertFalse(msg, region1Uri.equals(region2Uri));
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
