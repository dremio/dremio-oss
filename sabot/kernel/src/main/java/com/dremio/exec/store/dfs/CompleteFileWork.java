/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.store.dfs.easy.FileWork;
import com.dremio.exec.store.schedule.EndpointByteMap;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class CompleteFileWork implements FileWork, Comparable<CompleteFileWork>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompleteFileWork.class);

  private final long start;
  private final long length;
  private final String path;
  private final EndpointByteMap byteMap;

  public CompleteFileWork(EndpointByteMap byteMap, long start, long length, String path) {
    super();
    this.start = start;
    this.length = length;
    this.path = path;
    this.byteMap = byteMap;
  }

  public int compareTo(CompleteFileWork o) {
    if(o instanceof CompleteFileWork){
      CompleteFileWork c = (CompleteFileWork) o;
      int cmp = path.compareTo(c.getPath());
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
  public String getPath() {
    return path;
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
    return new FileWorkImpl(start, length, path);
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
        Objects.equal(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(start, length, path);
  }

  public static class FileWorkImpl implements FileWork{

    public long start;
    public long length;
    public String path;

    @JsonCreator
    public FileWorkImpl(@JsonProperty("start") long start, @JsonProperty("length") long length, @JsonProperty("path") String path) {
      super();
      this.start = start;
      this.length = length;
      this.path = path;
    }

    @Override
    public String getPath() {
      return path;
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
    return String.format("File: %s start: %d length: %d", path, start, length);
  }
}
