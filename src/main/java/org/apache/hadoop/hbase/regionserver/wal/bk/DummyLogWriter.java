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

public class DummyLogWriter implements HLog.Writer {

    private static final Log LOG = LogFactory.getLog(DummyLogWriter.class);

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
    public void sync() throws IOException {
      // FIXME (Fran): Is it possible to perform explicit sync on BK ???
    }

    /*
     */
    @Override
    public void append(Entry entry) throws IOException {
//      try {
//        lh.addEntry(Writables.getBytes(entry)); // XXX Use async ???
//      } catch (InterruptedException ie) {
//        Thread.currentThread().interrupt();
//        LOG.error("Interrupted ", ie);
//        throw new IOException();
//      } catch (BKException bke) {
//        LOG.error("Error closing handle", bke);
//        throw new IOException();
//      }
      if (lh == null) {
        throw new IOException();
      }
      entriesWrittenCount.incrementAndGet();
    }

    /*
     */
    @Override
    public long getLength() throws IOException {
//      return lh.getLastAddConfirmed();
      return entriesWrittenCount.get();
    }

}
