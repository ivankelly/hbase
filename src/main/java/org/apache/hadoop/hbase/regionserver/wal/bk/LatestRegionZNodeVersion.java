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

import java.util.concurrent.ConcurrentHashMap;
import java.net.URI;
import java.io.IOException;

/**
 * This keeps track of the latest version of the RegionZnode which
 * the inmemory state is uptodate with.
 * This should be updated when a set of logs are reads from a region znode
 * or when a new log is added to a region znode
 * This is a load of horrible static kludge, but its hard to get
 * rid of without a full refactor
 */
class LatestRegionZNodeVersion {
  static ConcurrentHashMap<String,Long> versions = new ConcurrentHashMap<String,Long>();

  public static Long get(URI regionURI) throws IOException {
    String znode = BookKeeperHLogHelper.znodeForRegionLogs(regionURI);
    return versions.get(znode);
  }

  public static void update(URI regionURI, long version) throws IOException {
    String znode = BookKeeperHLogHelper.znodeForRegionLogs(regionURI);
    versions.put(znode, version);
  }
}
