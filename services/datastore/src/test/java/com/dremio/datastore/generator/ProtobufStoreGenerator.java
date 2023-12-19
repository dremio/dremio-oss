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
package com.dremio.datastore.generator;

import java.util.Comparator;

import com.dremio.datastore.proto.Dummy;
import com.dremio.datastore.proto.Dummy.DummyId;
import com.dremio.datastore.proto.Dummy.DummyObj;
import com.google.common.collect.ImmutableList;

/**
 * Generates random, non-null protobuf values.
 */
public class ProtobufStoreGenerator extends ProtoGeneratorMixin implements DataGenerator<DummyId, DummyObj> {

  private DummyId newDummyId() {
    return DummyId.newBuilder()
      .setId(getString())
      .build();
  }

  @Override
  public DummyId newKey() {
    return newDummyId();
  }

  @Override
  public DummyObj newVal() {
    return DummyObj.newBuilder()
      .setId(newDummyId())
      .addAllIdList(ImmutableList.of(newDummyId(), newDummyId(), newDummyId()))
      .addAllInt32Seq(getInt32List())
      .addAllInt64Seq(getInt64List())
      .addAllUint32Seq(getInt32List())
      .addAllUint64Seq(getInt64List())
      .addAllFloatSeq(getFloatList())
      .addAllDoubleSeq(getDoubleList())
      .setType(getBool() ? Dummy.enumType.ZERO : Dummy.enumType.ONE)
      .setFlag(getBool())
      .build();
  }

  @Override
  public DummyObj newValWithNullFields() {
    throw new UnsupportedOperationException("Null fields are not supported in Protobuf.");
  }

  @Override
  public Comparator<DummyId> getComparator() {
    return new DummyIdProtobufComparator();
  }

  /**
   * Comparator class that compares protobuf DummyId objects.
   */
  private static final class DummyIdProtobufComparator implements Comparator<DummyId> {
    @Override
    public int compare(DummyId o1, DummyId o2) {
      return o1.getId().compareTo(o2.getId());
    }
  }
}
