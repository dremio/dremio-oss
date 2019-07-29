/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.dfs;

import org.apache.hadoop.fs.FileStatus;

import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.google.common.base.Objects;

public class CompleteFileWork implements FileWork, Comparable<CompleteFileWork>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompleteFileWork.class);

  private final long start;
  private final long length;
  private final FileStatus status;
  private final EndpointByteMap byteMap;

  public CompleteFileWork(EndpointByteMap byteMap, long start, long length, FileStatus status) {
    super();
    this.start = start;
    this.length = length;
    this.status = status;
    this.byteMap = byteMap;
  }

  public int compareTo(CompleteFileWork o) {
    if(o instanceof CompleteFileWork){
      CompleteFileWork c = (CompleteFileWork) o;
      int cmp = status.getPath().compareTo(c.status.getPath());
      if(cmp != 0){
        return cmp;
      }

      cmp = Long.compare(start,  c.getStart());
      if(cmp != 0){
        return cmp;
      }

    }

    return Long.compare(getTotalBytes(), o.getTotalBytes());

  }

  public EndpointByteMap getByteMap(){
    return byteMap;
  }

  public long getTotalBytes() {
    return length;
  }

  @Override
  public FileStatus getStatus() {
    return status;
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getLength() {
    return length;
  }

  public FileWorkImpl getAsFileWork(){
    return new FileWorkImpl(start, length, status);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompleteFileWork that = (CompleteFileWork) o;
    return start == that.start &&
        length == that.length &&
        Objects.equal(status, that.status);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, length, status);
  }

  public static class FileWorkImpl implements FileWork {

    public long start;
    public long length;
    public FileStatus status;

    public FileWorkImpl(long start, long length, FileStatus status) {
      super();
      this.start = start;
      this.length = length;
      this.status = status;
    }

    @Override
    public FileStatus getStatus() {
      return status;
    }

    @Override
    public long getStart() {
      return start;
    }

    @Override
    public long getLength() {
      return length;
    }

  }

  @Override
  public String toString() {
    return String.format("File: %s start: %d length: %d", status.getPath(), start, length);
  }
}
