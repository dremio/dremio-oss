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

import com.dremio.datastore.proto.DummyId;
import com.dremio.datastore.proto.DummyObj;
import com.dremio.datastore.proto.enumType;
import com.google.common.collect.ImmutableList;

/**
 * Used to test protostuff kv store format.
 */
public class ProtostuffStoreGenerator extends ProtoGeneratorMixin implements DataGenerator<DummyId, DummyObj> {

  private DummyId newDummyId() {
    return new DummyId(getString());
  }

  @Override
  public DummyId newKey() {
    return newDummyId();
  }

  @Override
  public DummyObj newVal() {
    return new DummyObj(newDummyId())
      .setFlag(getBool())
      .setIdListList(ImmutableList.of(newDummyId(), newDummyId(), newDummyId()))
      .setInt32SeqList(getInt32List())
      .setInt64SeqList(getInt64List())
      .setUint32SeqList(getInt32List())
      .setUint64SeqList(getInt64List())
      .setFloatSeqList(getFloatList())
      .setDoubleSeqList(getDoubleList())
      .setType(getBool() ? enumType.ZERO : enumType.ONE);
  }

  @Override
  public DummyObj newValWithNullFields() {
    return new DummyObj(newDummyId());
  }

  @Override
  public Comparator<DummyId> getComparator() {
    return new DummyIdProtostuffComparator();
  }

  /**
   * Comparator class that compares protostuff DummyId objects.
   */
  private static final class DummyIdProtostuffComparator implements Comparator<DummyId> {
    @Override
    public int compare(DummyId o1, DummyId o2) {
      return o1.getId().compareTo(o2.getId());
    }
  }
}
