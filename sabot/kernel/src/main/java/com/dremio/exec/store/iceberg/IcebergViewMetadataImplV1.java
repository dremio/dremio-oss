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
package com.dremio.exec.store.iceberg;

import com.dremio.common.exceptions.UserException;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

public class IcebergViewMetadataImplV1 implements IcebergViewMetadata {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(IcebergViewMetadataImplV1.class);
  private ViewMetadata viewMetadataV1;
  private SQLViewRepresentation sqlViewRepresentation = null;

  private IcebergViewMetadataImplV1(final ViewMetadata viewMetadataV1) {
    this.viewMetadataV1 = viewMetadataV1;
  }

  public static IcebergViewMetadataImplV1 of(final ViewMetadata viewMetadataV1) {
    return new IcebergViewMetadataImplV1(viewMetadataV1);
  }

  public static ViewMetadata getViewMetadata(final IcebergViewMetadata icebergViewMetadata) {
    Preconditions.checkState(
        icebergViewMetadata.getFormatVersion() == SupportedIcebergViewSpecVersion.V1);
    return ((IcebergViewMetadataImplV1) icebergViewMetadata).viewMetadataV1;
  }

  @Override
  public SupportedIcebergViewSpecVersion getFormatVersion() {
    return IcebergViewMetadata.of(viewMetadataV1.formatVersion());
  }

  @Override
  public Schema getSchema() {
    return viewMetadataV1.schema();
  }

  @Override
  public String getSql() {
    // TODO(DX-89378) : Need to handle different representations here
    return getValidSqlViewRepresentation().sql();
  }

  @Override
  public List<String> getSchemaPath() {
    return Arrays.asList(viewMetadataV1.currentVersion().defaultNamespace().levels());
  }

  @Override
  public String getLocation() {
    return viewMetadataV1.location();
  }

  @Override
  public String getMetadataLocation() {
    return viewMetadataV1.metadataFileLocation();
  }

  @Override
  public String getUniqueId() {
    return viewMetadataV1.uuid();
  }

  @Override
  public Map<String, String> getProperties() {
    return viewMetadataV1.properties();
  }

  @Override
  public long getCreatedAt() {
    return viewMetadataV1.history().get(0).timestampMillis();
  }

  @Override
  public long getLastModifiedAt() {
    return viewMetadataV1.currentVersion().timestampMillis();
  }

  @Override
  public String getDialect() {
    // TODO(DX-89378) : Need to handle different representations here
    return getValidSqlViewRepresentation().dialect();
  }

  @Override
  public String toJson() {
    return JsonUtil.generate(gen -> ViewMetadataParser.toJson(viewMetadataV1, gen), true);
  }

  @Override
  public IcebergViewMetadata fromJson(String metadataLocation, String json) {
    viewMetadataV1 = ViewMetadataParser.fromJson(json);
    return this;
  }

  private Set<org.apache.iceberg.view.SQLViewRepresentation> getSQLRepresentations() {
    return viewMetadataV1.currentVersion().representations().stream()
        .filter(
            representation ->
                representation instanceof org.apache.iceberg.view.SQLViewRepresentation)
        .map(representation -> (org.apache.iceberg.view.SQLViewRepresentation) representation)
        .collect(Collectors.toSet());
  }

  private Optional<SQLViewRepresentation> validateAndGetSqlViewRepresentation() {
    // This method currently returns the  first instance of SQLViewRepresentation that is supported
    // by Dremio
    // If not found, it continues checking for other suppported dialects and returns the first
    // supported instance.
    // Ideally we want to return the SQLViewRepresentation that is
    // requested by the user in syntax

    // Created a sorted list of the dialects. DremioSQL should be the first dialect to be tried.
    List<String> sortedListOfSupportedDialectsForReadAsString =
        EnumSet.allOf(IcebergViewMetadata.SupportedViewDialectsForRead.class).stream()
            .map(Enum::toString)
            .sorted()
            .collect(Collectors.toList());
    Set<SQLViewRepresentation> setOfSQLRepresentations = getSQLRepresentations();
    if (setOfSQLRepresentations.isEmpty()) {
      throw UserException.validationError()
          .message(
              "This view contains unsupported view representations. Only SQL representations are currently supported. Check the view metadata file for more details.")
          .build(logger);
    }
    // Find the first SQLViewRepresentation whose dialect matches any of the dialects in
    // sortedListOfSupportedDialectsForReadAsString in order.
    Optional<SQLViewRepresentation> representationOpt =
        sortedListOfSupportedDialectsForReadAsString.stream()
            .flatMap(
                dialect ->
                    setOfSQLRepresentations.stream()
                        .filter(
                            representation -> dialect.equalsIgnoreCase(representation.dialect())))
            .findFirst();

    if (representationOpt.isPresent()) {
      return representationOpt;
    }
    List<String> actualDialects =
        setOfSQLRepresentations.stream()
            .map(SQLViewRepresentation::dialect)
            .collect(Collectors.toList());

    List<String> supportedDialects =
        Arrays.stream(IcebergViewMetadata.SupportedViewDialectsForRead.values())
            .map(Enum::name)
            .collect(Collectors.toList());

    throw UserException.validationError()
        .message(
            "This view contains unsupported SQL dialects. Supported dialects are: %s. Actual dialects in representations: %s",
            supportedDialects, actualDialects)
        .build(logger);
  }

  private SQLViewRepresentation getValidSqlViewRepresentation() {
    if (sqlViewRepresentation != null) {
      return sqlViewRepresentation;
    }
    validateAndGetSqlViewRepresentation()
        .ifPresent(representation -> sqlViewRepresentation = representation);
    return sqlViewRepresentation;
  }
}
