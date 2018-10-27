/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.namespace;

import java.io.IOException;

import com.dremio.datastore.Serializer;
import com.dremio.datastore.StringSerializer;

/**
 * Dataset Split id serializer
 */
public class DatasetSplitIdSerializer extends Serializer<DatasetSplitId> {

  @Override
  public String toJson(DatasetSplitId v) throws IOException {
    return StringSerializer.INSTANCE.toJson(v.toString());
  }

  @Override
  public DatasetSplitId fromJson(String v) throws IOException {
    return DatasetSplitId.of(StringSerializer.INSTANCE.fromJson(v));
  }

  @Override
  public byte[] convert(DatasetSplitId v) {
    return StringSerializer.INSTANCE.convert(v.getSplitId());
  }

  @Override
  public DatasetSplitId revert(byte[] v) {
    return DatasetSplitId.of(StringSerializer.INSTANCE.revert(v));
  }
}
