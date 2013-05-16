package org.apache.hadoop.hbase.regionserver.wal.bk;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.Writables;

public class DummyLogWriter implements HLog.Writer {

  private static final Log LOG = LogFactory.getLog(DummyLogWriter.class);
  // Hashmap mapping from URI to writer
  // A test accesses this map and verifies what has been written to it
  final static ConcurrentHashMap<URI, Writer> regionWriters = new ConcurrentHashMap<URI, Writer>();
    
  private final AtomicLong entriesWrittenCount = new AtomicLong(0);

  private boolean initialized = false;
  /*
   */
  @Override
  public void init(FileSystem fs, URI uri, Configuration c) throws IOException {
    LOG.info("Initializing for URI " + uri);
    if(uri == null) {
        throw new IOException("URI should not be null");
    }
    // Add this writer to map
    regionWriters.put(uri, this);
    initialized = true;
  }

  /*
   */
  @Override
  public void close() throws IOException {
    LOG.info("Closing");
    initialized = false;
  }

  /*
   */
  @Override
  public void sync() throws IOException {
    stopIfNotInitialized();
//    LOG.info("Synchronizing");
  }

  /*
   */
  @Override
  public void append(Entry entry) throws IOException {
    stopIfNotInitialized();
    entriesWrittenCount.incrementAndGet();
    LOG.info("Append entry " + entry);
  }

  /*
   */
  @Override
  public long getLength() throws IOException {
    return entriesWrittenCount.get();
  }
  
  private void stopIfNotInitialized() throws IOException {
    if ( ! initialized ) {
      throw new IOException("Please initialize first");
    }
  }

}