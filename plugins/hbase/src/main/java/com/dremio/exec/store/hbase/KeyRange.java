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
package com.dremio.exec.store.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.dremio.hbase.proto.HBasePluginProto.HBaseSplitXattr;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * An HBase start top key range.
 */
public class KeyRange {

  private final Range<ByteArr> range;

  private KeyRange(Range<ByteArr> range) {
    super();
    this.range = range;
  }

  public boolean overlaps(KeyRange range) {
    return this.range.isConnected(range.range);
  }

  public byte[] getStart() {
    return range.lowerEndpoint().bytes;
  }

  public byte[] getStop() {
    ByteArr arr = range.upperEndpoint();
    if(arr.bytes == null) {
      return new byte[0];
    }
    return arr.bytes;
  }

  public static KeyRange getRange(byte[] startRow, byte[] stopRow){
    return new KeyRange(Range.openClosed(new ByteArr(startRow, true), new ByteArr(stopRow, false)));
  }

  public static KeyRange getRange(ByteString startRow,ByteString stopRow){
    return new KeyRange(Range.openClosed(new ByteArr(startRow == null ? null : startRow.toByteArray(), true), new ByteArr(stopRow == null ? null : stopRow.toByteArray(), false)));
  }

  public static KeyRange fromSplit(DatasetSplit input) {
    ByteString prop = input.getSplitExtendedProperty();
    try {
      HBaseSplitXattr split = HBaseSplitXattr.parseFrom(prop);
      return getRange(split.getStart(), split.getStop());
    } catch (InvalidProtocolBufferException e) {
      throw Throwables.propagate(e);
    }
  }

  public KeyRange intersection(KeyRange range) {
    return new KeyRange(this.range.intersection(range.range));
  }

  private static class ByteArr implements Comparable<ByteArr> {
    private final byte[] bytes;

    private ByteArr(byte[] bytes, boolean start) {
      super();
      if(bytes == null && start) {
        bytes = new byte[0];
      }

      if(!start && bytes != null && bytes.length == 0) {
        bytes = null;
      }
      this.bytes = bytes;
    }

    @Override
    public int compareTo(ByteArr o) {
      if(bytes == null && o.bytes == null) {
        return 0;
      }
      if(bytes == null) {
        return 1;
      }

      if(o.bytes == null) {
        return -1;
      }

      return Bytes.compareTo(bytes, o.bytes);
    }

  }

}
