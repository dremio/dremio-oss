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
package com.dremio.exec.physical.config;

import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.util.LongRange;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.iceberg.expressions.Expression;
import org.immutables.value.Value;

/** Filter conditions for the manifest scan */
@JsonDeserialize(builder = ImmutableManifestScanFilters.Builder.class)
@Value.Immutable
public abstract class ManifestScanFilters implements MetadataFilters {

  // Col filter expression represented in org.apache.iceberg.expressions.Expression
  @Nullable
  public abstract byte[] getIcebergAnyColExpression();

  // Filter the files within this file size range with end values inclusive. Ignored if null.
  @Nullable
  public abstract LongRange getSkipDataFileSizeRange();

  // Filter to identify if manifest is operating on an old partition spec id. Ignored if null or
  // zero.
  @Value.Default
  public Integer getMinPartitionSpecId() {
    return 0;
  }

  public static ManifestScanFilters empty() {
    return new ImmutableManifestScanFilters.Builder().build();
  }

  public boolean doesIcebergAnyColExpressionExists() {
    return getIcebergAnyColExpression() != null && getIcebergAnyColExpression().length > 0;
  }

  public boolean doesSkipDataFileSizeRangeExist() {
    return getSkipDataFileSizeRange() != null
        && !getSkipDataFileSizeRange().equals(new LongRange(0, 0));
  }

  public boolean doesMinPartitionSpecIdExist() {
    return getMinPartitionSpecId() != null && getMinPartitionSpecId() > 0;
  }

  @JsonIgnore
  public Expression getIcebergAnyColExpressionDeserialized() {
    try {
      return IcebergSerDe.deserializeFromByteArray(getIcebergAnyColExpression());
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Failed to deserialize ManifestFile Filter AnyColExpression", e);
    }
  }
}
