package org.apache.hadoop.hbase.regionserver.wal.bk;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.net.URI;
import java.util.Map;
import java.util.NavigableSet;

import org.junit.Test;

public class TestBKUriDummyImpl {
  private static final Log LOG = LogFactory.getLog(TestDummyLogCluster.class);

  @Test(timeout=60000)
  public void testRegionLogZK() throws Exception {
    BookKeeperHLogHelper.reset();
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    URI uri = BookKeeperHLogHelper.regionUriFromBase(URI.create("bookkeeper:/hbasewal"),
                                                     "testTable", "testRegion");
    testUtil.startMiniZKCluster();
    BookKeeperHLogHelper.addLogForRegion(conf, uri, 1, 1);
    BookKeeperHLogHelper.addLogForRegion(conf, uri, 2, 2);
    BookKeeperHLogHelper.addLogForRegion(conf, uri, 3, 3);

    NavigableSet<URI> uris = BookKeeperHLogHelper.listLogs(conf, uri);
    assertEquals("There should be 3", uris.size(), 3);
    int i = 1;
    for (URI u : uris) {
      assertEquals("Wrong ledger id", BookKeeperHLogHelper.getLedgerIdForURI(conf, u), i++);
    }

    testUtil.shutdownMiniZKCluster();
  }

  @Test(timeout=60000)
  public void testCluster() throws Exception {
    BookKeeperHLogHelper.reset();
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    conf.setBoolean("dfs.support.append", true);
    // The below config supported by 0.20-append and CDH3b2
    conf.setInt("dfs.client.block.recovery.retries", 2);
    byte[] colFam = "DummyFamily".getBytes();
    byte[] col = "DummyCol".getBytes();
    String baseUri = "bookkeeper:/hbasewal";
    String tableName = "testTable";
    String region = "testRegion";

    conf = HBaseConfiguration.addHbaseResources(conf);

    conf.set(HConstants.HBASE_WAL_BASEURI, baseUri);
    conf.set("hbase.regionserver.hlog.reader.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader");
    conf.set("hbase.regionserver.hlog.writer.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogWriter");
    testUtil.startMiniCluster();

    HTable table = testUtil.createTable(tableName.getBytes(), colFam);

    int count = 0;
    for (Map.Entry<URI, Writer> e: DummyLogWriter.regionWriters.entrySet()) {
      LOG.info("URI " + e.getKey()
               + " Writer " + ((DummyLogWriter)e.getValue()).getLength());
      count++;
    }

    Put p = new Put(Bytes.toBytes("testPutWithBKWAL"));
    p.add(colFam, col, Bytes.toBytes("testPutWithBKWAL-val"));
    table.put(p);

    Get g = new Get("testPutWithBKWAL".getBytes());
    Result res = table.get(g);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "testPutWithBKWAL-val");

    int count2 = 0;
    for (Map.Entry<URI, Writer> e: DummyLogWriter.regionWriters.entrySet()) {
      LOG.info("URI2 " + e.getKey()
               + " Writer " + ((DummyLogWriter)e.getValue()).getLength());
      count2++;
    }
    assertEquals("There should be another log ", count2, count+1);

    testUtil.shutdownMiniCluster();
  }
}
