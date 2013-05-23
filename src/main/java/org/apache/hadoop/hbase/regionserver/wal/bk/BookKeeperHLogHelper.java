/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal.bk;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import java.util.NavigableSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URI;

public class BookKeeperHLogHelper {
  private static final Log LOG = LogFactory.getLog(BookKeeperHLogHelper.class);

  private static RecoverableZooKeeper zk = null;

  synchronized RecoverableZooKeeper getZK(Configuration conf)
      throws IOException {
    try {
      if (zk == null) {
        BKHLogWatcher watcher = new BKHLogWatcher();
        RecoverableZooKeeper newzk = new RecoverableZooKeeper(ZKConfig.getZKQuorumServersString(conf),
                                                              60000, watcher, 5, 10000);
        watcher.setZooKeeper(newzk);
        watcher.awaitConnected();
        zk = newzk;
      }
      return zk;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted getting zk instance", ie);
    }
  }

  public static class LogSpec {
    final String table;
    final String region;
    final long seqNum;

    LogSpec(String table, String region, long seqNum) {
      this.table = table;
      this.region = region;
      this.seqNum = seqNum;
    }
  
    LogSpec(String table, String region) {
      this.table = table;
      this.region = region;
      this.seqNum = -1;
    }
    
    public boolean hasSeqNum() {
      return seqNum != -1;
    }
    
    public String getTable() {
      return table;
    }

    public String getRegion() {
      return region;
    }

    public long getSeqNum() {
      return seqNum;
    }
  }

  private final static Pattern uriPattern = Pattern.compile("/t:([^/]+)/r:([^/]+)/?((\\d)+)?");
  public static LogSpec parseRegionUri(URI regionUri) throws IOException {
    Matcher m = uriPattern.matcher(regionUri.toString());
    if (m.find()) {
      if (m.group(3) != null) {
        return new LogSpec(m.group(1), m.group(2), Long.valueOf(m.group(3)));
      } else {
        return new LogSpec(m.group(1), m.group(2));
      }
    }
    throw new IOException("Cannot parse URI " + regionUri);
  }

  private static String regionUriString(String base, String table, String region) {
    if (!base.endsWith("/")) {
      base = base + "/";
    }
    return String.format("%s/t:%s/r:%s", base, table, region);
  }

  public static URI regionUriFromBase(URI base, String table, String region) {
    return URI.create(regionUriString(base.toString(), table, region));
  }

  public static URI logUriFromBase(URI base, String table, String region, long seqNum) {
    return logUriFromRegionUri(URI.create(regionUriString(base.toString(),table,region)), seqNum);
  }

  public static URI logUriFromRegionUri(URI regionUri, long seqNum) {
    return URI.create(regionUri.toString() + String.format("/%020d", seqNum));
  }

  public static URI logUriFromRegionUriWithQS(URI regionUri, long seqNum, String QS) {
    return URI.create(logUriFromRegionUri(regionUri, seqNum).toString() + QS);
  }

  public static NavigableSet<URI> listLogs(Configuration conf, URI regionlogsURI)
      throws IOException {
    
    return null;
  }

  static class BKHLogWatcher implements Watcher {
    RecoverableZooKeeper handle = null;
    CountDownLatch connectLatch = new CountDownLatch(1);

    void setZooKeeper(RecoverableZooKeeper handle) {
      this.handle = handle;
    }

    void awaitConnected() throws InterruptedException, IOException {
      if (!connectLatch.await(10, TimeUnit.SECONDS)) {
        throw new IOException("Could not connect to zookeeper");
      }
    }
    
    public void process(WatchedEvent event) {
      LOG.info("Received ZooKeeper Event, " +
               "type=" + event.getType() + ", " +
               "state=" + event.getState() + ", " +
               "path=" + event.getPath());
      switch (event.getState()) {
      case SyncConnected:
        connectLatch.countDown();
        break;
      case Expired:
        LOG.error("ZooKeeper client connection to the "
                  + "ZooKeeper server has expired!");
        if (handle != null) {
          new Thread("zk-reconnect-thread") {
            public void run() {
              synchronized (BKHLogWatcher.this) {
                try {
                  handle.reconnectAfterExpiration();
                } catch (IOException ioe) {
                  LOG.error("IOException reconnecting to zookeeper", ioe);
                } catch (InterruptedException ie) {
                  Thread.currentThread().interrupt();
                  LOG.error("Interrupted reconnecting to zookeeper", ie);
                }
              }
            }
          }.start();
        }
        break;
      }
    }
  }
}
