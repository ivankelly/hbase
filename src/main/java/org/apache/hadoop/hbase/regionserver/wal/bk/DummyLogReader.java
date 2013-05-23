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
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Writables;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class DummyLogReader implements HLog.Reader {

  private static final Log LOG = LogFactory.getLog(DummyLogReader.class);

  private static final String PWD = "pwd";

  private String keyprefix = "";
  private long read = 0;
  private long count = 100;
  private long seqid = 0;
  private byte[] table = null;
  private byte[] region = null;

  /*
   * Format is dummy://<table>/<region>/<sequence>?keyprefix=<prefix>&count=<count>
   * the key prefix is a prefix for the keys that this reader will
   * allow to be read. the count is the number of keys to have in the log.
   */
  @Override
  public void init(FileSystem fs, URI uri, Configuration c)
    throws IOException {
    if (!uri.getScheme().equals("dummy")) {
      throw new IOException("Invalid uri scheme " + uri.getScheme());
    }

    BookKeeperHLogHelper.LogSpec spec = BookKeeperHLogHelper.parseRegionUri(uri);

    table = spec.getTable().getBytes();
    region = spec.getRegion().getBytes();
    if (!spec.hasSeqNum()) {
      throw new IOException("URI has no sequence number: "+ uri);
    }
    seqid = spec.getSeqNum();

    Pattern p = Pattern.compile("\\?(.*)$");
    Matcher m = p.matcher(uri.toString());
    if (m.find()) {
      String[] query = m.group(1).split("&");
      for (String item : query) {
        String[] itemparts = item.split("=");
        if (itemparts.length != 2) {
          throw new IOException("Invalid query string item " + item);
        }
        if (itemparts[0].equals("keyprefix")) {
          keyprefix = itemparts[1];
        } else if (itemparts[0].equals("count")) {
          count = Long.valueOf(itemparts[1]);
        }
      }
    }
  }

  /*
   */
  @Override
  public void close() throws IOException {
  }

  /*
   */
  @Override
  public Entry next() throws IOException {
    if (read >= count) {
      return null;
    }

    HLogKey key = new HLogKey(region, table,
                              seqid + read, HConstants.LATEST_TIMESTAMP,
                              HConstants.DEFAULT_CLUSTER_ID);
    WALEdit edit = new WALEdit();
    KeyValue kv = new KeyValue((keyprefix + "-" + (seqid + read)).getBytes(),
                               "DummyFamily".getBytes(), "DummyCol".getBytes(),
                               System.currentTimeMillis(),
                               ("Value-" + (seqid + read)).getBytes());
    edit.add(kv);
    Entry e = new Entry(key, edit);
    read++;

    return e;
  }

  /*
   */
  @Override
  public Entry next(Entry reuse) throws IOException {
    return next(); // since dummy isn't reading from serialized, we can't reuse
  }

  /*
   */
  @Override
  public void seek(long pos) throws IOException {
    read = pos;
  }

  /*
   */
  @Override
  public long getPosition() throws IOException {
    return read;
  }

}
