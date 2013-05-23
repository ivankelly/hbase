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

import org.junit.Test;

public class TestDummyLogCluster {
  private static final Log LOG = LogFactory.getLog(TestDummyLogCluster.class);

  @Test
  public void testCluster() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    conf.setBoolean("dfs.support.append", true);
    // The below config supported by 0.20-append and CDH3b2
    conf.setInt("dfs.client.block.recovery.retries", 2);
    byte[] colFam = "DummyFamily".getBytes();
    byte[] col = "DummyCol".getBytes();

    conf = HBaseConfiguration.addHbaseResources(conf);
    conf.set(HConstants.HBASE_WAL_BASEURI, "dummy:test");
    conf.set("hbase.regionserver.hlog.reader.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogReader");
    conf.set("hbase.regionserver.hlog.writer.impl",
             "org.apache.hadoop.hbase.regionserver.wal.bk.DummyLogWriter");
    testUtil.startMiniCluster();

    HTable table = testUtil.createTable("testTable".getBytes(), colFam);

    Get g = new Get("foobar-1".getBytes());
    Result res = table.get(g);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-1");

    g = new Get("foobar-10".getBytes());
    res = table.get(g);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-10");

    g = new Get("foobar-11".getBytes());
    res = table.get(g);
    assertFalse("Should not have column", res.containsColumn(colFam, col));

    g = new Get("foobaz-200".getBytes());
    res = table.get(g);
    assertFalse("Should not have column", res.containsColumn(colFam, col));

    g = new Get("foobaz-201".getBytes());
    res = table.get(g);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-201");

    g = new Get("foobaz-300".getBytes());
    res = table.get(g);
    assertTrue("Should have column", res.containsColumn(colFam, col));
    assertEquals("Should match", new String(res.getValue(colFam, col)), "Value-300");

    g = new Get("foobaz-301".getBytes());
    res = table.get(g);
    assertFalse("Should not have column", res.containsColumn(colFam, col));

    int count = 0;
    for (Map.Entry<URI, Writer> e: DummyLogWriter.regionWriters.entrySet()) {
      LOG.info("URI " + e.getKey()
               + " Writer " + ((DummyLogWriter)e.getValue()).getLength());
      count++;
    }
    Put p = new Put(Bytes.toBytes("foobaz-202"));
    p.add(colFam, col, Bytes.toBytes("Value-test"));

    table.put(p);

    int count2 = 0;
    for (Map.Entry<URI, Writer> e: DummyLogWriter.regionWriters.entrySet()) {
      LOG.info("URI2 " + e.getKey()
               + " Writer " + ((DummyLogWriter)e.getValue()).getLength());
      count2++;
    }
    assertEquals("There should be another log ", count2, count+1);
  }
}
