package org.apache.hadoop.hbase.regionserver.wal.bk;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

//import org.apache.bookkeeper.client.BKException;
//import org.apache.bookkeeper.client.BookKeeper;
//import org.apache.bookkeeper.client.BookKeeper.DigestType;
//import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Writables;

public class DummyLogReader implements HLog.Reader {

  private static final Log LOG = LogFactory.getLog(DummyLogReader.class);

  private static final String PWD = "pwd";
    
  private final AtomicLong entriesWrittenCount = new AtomicLong(0);
    
//    private BookKeeper bk;
//    private LedgerHandler lh;
  private Object lh = null; // FIXME (Fran): Remove me
    
  /*
   */
  @Override
  public void init(FileSystem fs, URI uri, Configuration c)
    throws IOException {
//      ClientConfiguration conf = new ClientConfiguration(); // FIXME (Fran): Check BK Configuration
//      conf.setZkServers(c.get("hbase.zookeeper.quorum"));
//      conf.setReadTimeout(100000000);
//
//      bk = new BookKeeper(conf);
//      lh = bk.createLedger(3, 3, DigestType.CRC32, PWD.getBytes());
    lh = new Object();
  }

  /*
   */
  @Override
  public void close() throws IOException {
    lh = null;
//      try {
//        lh.close();
//      } catch (InterruptedException ie) {
//        Thread.currentThread().interrupt();
//        LOG.error("Interrupted ", ie);
//      } catch (BKException bke) {
//        LOG.error("Error closing handle", bke);
//      }
  }

  /*
   */
  @Override
  public Entry next() throws IOException {
    return null;
  }

  /*
   */
  @Override
  public Entry next(Entry reuse) throws IOException {
    return null;
  }

  /*
   */
  @Override
  public void seek(long pos) throws IOException {
  }

  /*
   */
  @Override
  public long getPosition() throws IOException {
    return 0;
  }

}
