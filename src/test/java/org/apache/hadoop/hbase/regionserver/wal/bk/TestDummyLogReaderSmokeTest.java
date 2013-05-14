package org.apache.hadoop.hbase.regionserver.wal.bk;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestDummyLogReaderSmokeTest {

  private static final Log LOG = LogFactory.getLog(TestDummyLogReaderSmokeTest.class);

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

    FileSystem fs = TEST_UTIL.getDFSCluster().getFileSystem();
    Path hbaseRootDir = new Path(conf.get(HConstants.HBASE_DIR));
    if (TEST_UTIL.getDFSCluster().getFileSystem().exists(hbaseRootDir)) {
      TEST_UTIL.getDFSCluster().getFileSystem().delete(hbaseRootDir, true);
    }

    conf.set("hbase.regionserver.hlog.reader.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader");
  }

  @After
  public void tearDown() throws Exception {
  }
  
  @Test
  public void testSuccesfulLoadOfDummyLogReaderFromConfiguration() throws Exception {
    Class<? extends Reader> logWriterClass = conf.getClass("hbase.regionserver.hlog.reader.impl", 
                                                          SequenceFileLogReader.class, Reader.class);
    assertEquals("org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader",
                 logWriterClass.getName());
  }
  
  /** Utility methods **/
  
  private HLog.Reader getDummyReaderInstance() throws InstantiationException, IllegalAccessException {
    Class<? extends Reader> logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl", 
          SequenceFileLogReader.class, Reader.class);
    return (HLog.Reader) logReaderClass.newInstance();
  }

  @Test
  public void testInitLogReader() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    HLog.Reader reader = HLog.getReader(fs,
        URI.create("dummy://table1/region2/00001?keyprefix=keya&count=10"), conf);
    for (int i = 1; i <= 10; i++) {
      Entry e = reader.next();
      assertNotNull("entry shouldn't be null", e);
      assertEquals("Key should be keya-" + i, "keya-" + i,
          new String(e.getEdit().getKeyValues().get(0).getRow()));
    }
    assertTrue("Should be no more entries", reader.next() == null);
  }

  @Test
  public void testInitHregion() throws Exception {
    byte[] tableName = "testInitHRegionTable".getBytes();
    byte[] colFam = "DummyFamily".getBytes();
    byte[] col = "DummyCol".getBytes();
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(colFam));

    HRegionInfo info = new HRegionInfo(htd.getName(), null/*startKey*/, null/*stopKey*/,
                                       false, (long)new String(tableName).hashCode());

    HRegion hregion = HRegion.openHRegion(info, htd, null, conf);
    Get g = new Get("foobar-1".getBytes());
    Result res = hregion.get(g, null);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-1");

    g = new Get("foobar-10".getBytes());
    res = hregion.get(g, null);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-10");

    g = new Get("foobar-11".getBytes());
    res = hregion.get(g, null);
    assertFalse("Should not have column", res.containsColumn(colFam, col));

    g = new Get("foobaz-200".getBytes());
    res = hregion.get(g, null);
    assertFalse("Should not have column", res.containsColumn(colFam, col));

    g = new Get("foobaz-201".getBytes());
    res = hregion.get(g, null);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-201");

    g = new Get("foobaz-300".getBytes());
    res = hregion.get(g, null);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-300");

    g = new Get("foobaz-301".getBytes());
    res = hregion.get(g, null);
    assertFalse("Should not have column", res.containsColumn(colFam, col));
  }
}
