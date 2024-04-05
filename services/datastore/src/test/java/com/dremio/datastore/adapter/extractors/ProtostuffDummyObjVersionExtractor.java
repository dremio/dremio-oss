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
package com.dremio.datastore.adapter.extractors;

import com.dremio.datastore.VersionExtractor;
import com.dremio.datastore.proto.DummyObj;

/** DummyObj VersionExtractor for Protostuff. */
public class ProtostuffDummyObjVersionExtractor implements VersionExtractor<DummyObj> {
  @Override
  public String getTag(DummyObj value) {
    return value.getTag();
  }

  @Override
  public void setTag(DummyObj value, String tag) {
    value.setTag(tag);
  }

  @Override
  public AutoCloseable preCommit(DummyObj value) {
    boolean negate = !value.getFlag();
    value.setFlag(negate);
    return () -> value.setFlag(!negate);
  }
}
