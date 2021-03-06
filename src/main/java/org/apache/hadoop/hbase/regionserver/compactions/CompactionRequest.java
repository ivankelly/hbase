/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;

  /**
   * This class represents a compaction request and holds the region, priority,
   * and time submitted.
   */
  public class CompactionRequest implements Comparable<CompactionRequest> {
    static final Log LOG = LogFactory.getLog(CompactionRequest.class);
    private final HRegion r;
    private final Store s;
    private int p;
    private final Date date;

    public CompactionRequest(HRegion r, Store s) {
      this(r, s, s.getCompactPriority());
    }

    public CompactionRequest(HRegion r, Store s, int p) {
      this(r, s, p, null);
    }

    public CompactionRequest(HRegion r, Store s, int p, Date d) {
      if (r == null) {
        throw new NullPointerException("HRegion cannot be null");
      }

      if (d == null) {
        d = new Date();
      }

      this.r = r;
      this.s = s;
      this.p = p;
      this.date = d;
    }

    /**
     * This function will define where in the priority queue the request will
     * end up.  Those with the highest priorities will be first.  When the
     * priorities are the same it will It will first compare priority then date
     * to maintain a FIFO functionality.
     *
     * <p>Note: The date is only accurate to the millisecond which means it is
     * possible that two requests were inserted into the queue within a
     * millisecond.  When that is the case this function will break the tie
     * arbitrarily.
     */
    @Override
    public int compareTo(CompactionRequest request) {
      //NOTE: The head of the priority queue is the least element
      if (this.equals(request)) {
        return 0; //they are the same request
      }
      int compareVal;

      compareVal = p - request.p; //compare priority
      if (compareVal != 0) {
        return compareVal;
      }

      compareVal = date.compareTo(request.date);
      if (compareVal != 0) {
        return compareVal;
      }

      //break the tie arbitrarily
      return -1;
    }

    /** Gets the HRegion for the request */
    public HRegion getHRegion() {
      return r;
    }

    /** Gets the Store for the request */
    public Store getStore() {
      return s;
    }

    /** Gets the priority for the request */
    public int getPriority() {
      return p;
    }

    /** Gets the priority for the request */
    public void setPriority(int p) {
      this.p = p;
    }

    public String toString() {
      return "regionName=" + r.getRegionNameAsString() +
        ((s == null) ? ""
                     : "storeName = " + new String(s.getFamily().getName())) +
        ", priority=" + p + ", date=" + date;
    }
  }
