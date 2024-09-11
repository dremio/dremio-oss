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
package com.dremio.sabot.op.writer;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;
import org.immutables.value.Value;

@JsonDeserialize(builder = ImmutableWriterCommitterRecord.Builder.class)
@Value.Immutable
public interface WriterCommitterRecord {

  @Nullable
  VarCharHolder fragment();

  @Nullable
  Long records();

  @Nullable
  VarCharHolder path();

  @Nullable
  VarBinaryHolder metadata();

  @Nullable
  Integer partition();

  @Nullable
  Long fileSize();

  @Nullable
  VarBinaryHolder icebergMetadata();

  @Nullable
  VarBinaryHolder fileSchema();

  @Nullable
  List<VarBinaryHolder> partitionData();

  @Nullable
  Integer operationType();

  @Nullable
  VarBinaryHolder referencedDataFiles();
}
