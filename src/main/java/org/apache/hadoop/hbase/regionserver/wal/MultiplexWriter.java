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
package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;

import java.util.concurrent.ConcurrentHashMap;
import java.net.URI;
import java.io.IOException;

import com.google.common.base.Charsets;

public class MultiplexWriter implements Writer {
  ConcurrentHashMap<String, Writer> regionWriters = new ConcurrentHashMap<String, Writer>();
  private FileSystem fs = null;
  private URI baseUri = null;
  private Configuration conf = null;

  public void init(FileSystem fs, URI baseUri, Configuration c) throws IOException {
    this.fs = fs;
    this.baseUri = baseUri;
    this.conf = c;
  }

  public void close() throws IOException {
    for(String key : regionWriters.keySet()) { // Remove and close all writers
      Writer writer = regionWriters.remove(key);
      writer.close();
    }
  }

  public void sync() throws IOException {
    for(String key : regionWriters.keySet()) {
      Writer writer = regionWriters.get(key);
      writer.sync();
    }
  }

  public void append(Entry entry) throws IOException {
    String region = new String(entry.getKey().getEncodedRegionName(), Charsets.UTF_8);
    Writer writer = regionWriters.get(region);
    if(writer == null) {
      // Create the right URI to Writer instance using logKey.getLogSeqNum()
      URI walUri = URI.create("dummy://"
                              + new String(entry.getKey().getTablename(), Charsets.UTF_8)
                              + "/" + region
                              + "/" + entry.getKey().getLogSeqNum());
      Writer newWriter = HLog.createWriter(fs, walUri, conf);
      writer = regionWriters.putIfAbsent(region, newWriter);
      if (writer == null) {
        writer = newWriter;
      }
    }
    writer.append(entry);
  }

  public long getLength() throws IOException {
    long len = 0;
    for(String key : regionWriters.keySet()) {
      len += regionWriters.get(key).getLength();
    }
    return len;
  }
}
