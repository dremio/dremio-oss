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
package com.dremio.exec.store.iceberg.dremioudf.core.udf;

import com.dremio.exec.store.iceberg.dremioudf.api.udf.ReplaceUdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfHistoryEntry;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UpdateUdfProperties;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.UpdateLocation;

public class BaseUdf implements Udf, Serializable {

  private final UdfOperations ops;
  private final String name;

  public BaseUdf(UdfOperations ops, String name) {
    this.ops = ops;
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  public UdfOperations operations() {
    return ops;
  }

  @Override
  public UdfVersion currentVersion() {
    return operations().current().currentVersion();
  }

  @Override
  public Iterable<UdfVersion> versions() {
    return operations().current().versions();
  }

  @Override
  public UdfVersion version(String versionId) {
    return operations().current().version(versionId);
  }

  @Override
  public Iterable<UdfSignature> signatures() {
    return operations().current().signatures();
  }

  @Override
  public UdfSignature signature(String signatureId) {
    return operations().current().signaturesById().get(signatureId);
  }

  @Override
  public List<UdfHistoryEntry> history() {
    return operations().current().history();
  }

  @Override
  public Map<String, String> properties() {
    return operations().current().properties();
  }

  @Override
  public String location() {
    return operations().current().location();
  }

  @Override
  public UpdateUdfProperties updateProperties() {
    return new PropertiesUpdate(ops);
  }

  @Override
  public ReplaceUdfVersion replaceVersion() {
    return new UdfVersionReplace(ops);
  }

  @Override
  public UpdateLocation updateLocation() {
    return new SetUdfLocation(ops);
  }

  @Override
  public UUID uuid() {
    return UUID.fromString(ops.current().uuid());
  }

  /**
   * This implementation of sqlFor will resolve what is considered the "closest" dialect. If an
   * exact match is found, then that is returned. Otherwise, the first representation would be
   * returned. If no SQL representation is found, null is returned.
   */
  @Override
  public SQLUdfRepresentation sqlFor(String dialect) {
    Preconditions.checkArgument(dialect != null, "Invalid dialect: null");
    Preconditions.checkArgument(!dialect.isEmpty(), "Invalid dialect: (empty string)");
    SQLUdfRepresentation closest = null;
    for (UdfRepresentation representation : currentVersion().representations()) {
      if (representation instanceof SQLUdfRepresentation) {
        SQLUdfRepresentation sqlUDFRepresentation = (SQLUdfRepresentation) representation;
        if (sqlUDFRepresentation.dialect().equalsIgnoreCase(dialect)) {
          return sqlUDFRepresentation;
        } else if (closest == null) {
          closest = sqlUDFRepresentation;
        }
      }
    }

    return closest;
  }
}
