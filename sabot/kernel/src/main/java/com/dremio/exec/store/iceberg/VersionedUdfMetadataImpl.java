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
import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadata;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadataParser;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class VersionedUdfMetadataImpl implements VersionedUdfMetadata {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(VersionedUdfMetadataImpl.class);
  private UdfMetadata udfMetadata;
  private SQLUdfRepresentation sqlUdfRepresentation;

  private VersionedUdfMetadataImpl(final UdfMetadata udfMetadata) {
    this.udfMetadata = udfMetadata;
  }

  public static VersionedUdfMetadataImpl of(final UdfMetadata udfMetadata) {
    return new VersionedUdfMetadataImpl(udfMetadata);
  }

  public static UdfMetadata getUdfMetadata(final VersionedUdfMetadata versionedUdfMetadata) {
    Preconditions.checkState(
        versionedUdfMetadata.getFormatVersion() == SupportedVersionedUdfSpecVersion.V0);
    return ((VersionedUdfMetadataImpl) versionedUdfMetadata).udfMetadata;
  }

  @Override
  public SupportedVersionedUdfSpecVersion getFormatVersion() {
    return SupportedVersionedUdfSpecVersion.of(udfMetadata.formatVersion());
  }

  @Override
  public String getComment() {
    return getValidSqlUdfRepresentation().comment();
  }

  @Override
  public List<String> getSchemaPath() {
    return Arrays.asList(udfMetadata.currentVersion().defaultNamespace().levels());
  }

  @Override
  public String getLocation() {
    return udfMetadata.location();
  }

  @Override
  public String getMetadataLocation() {
    return udfMetadata.metadataFileLocation();
  }

  @Override
  public String getUniqueId() {
    return udfMetadata.uuid();
  }

  @Override
  public Map<String, String> getProperties() {
    return udfMetadata.properties();
  }

  @Override
  public long getCreatedAt() {
    return udfMetadata.versions().get(0).timestampMillis();
  }

  @Override
  public long getLastModifiedAt() {
    return udfMetadata.currentVersion().timestampMillis();
  }

  @Override
  public String getDialect() {
    return getValidSqlUdfRepresentation().dialect();
  }

  @Override
  public String getBody() {
    return getValidSqlUdfRepresentation().body();
  }

  @Override
  public Type getReturnType() {
    return udfMetadata.signaturesById().get(udfMetadata.currentSignatureId()).returnType();
  }

  @Override
  public List<Types.NestedField> getParameters() {
    return udfMetadata.signaturesById().get(udfMetadata.currentSignatureId()).parameters();
  }

  @Override
  public String toJson() {
    return JsonUtil.generate(gen -> UdfMetadataParser.toJson(udfMetadata, gen), true);
  }

  @Override
  public VersionedUdfMetadata fromJson(String metadataLocation, String json) {
    udfMetadata = UdfMetadataParser.fromJson(json);
    return this;
  }

  // TODO : When we support substrait or other representations, this method would need to read the
  // representation and do a translation. Currently this assumes there is only support for
  // SQLRepresentation

  private Set<SQLUdfRepresentation> getSQLRepresentations() {
    return udfMetadata.currentVersion().representations().stream()
        .filter(representation -> representation instanceof SQLUdfRepresentation)
        .map(representation -> (SQLUdfRepresentation) representation)
        .collect(Collectors.toSet());
  }

  private Optional<SQLUdfRepresentation> validateAndGetSqlUdfRepresentation() {
    // This method currently returns the  first instance of SQLUdfRepresentation that is supported
    // by Dremio. It always first checks if DremioSQL is supported and then returns the first
    // instance of it.
    // If not found, it continues checking for other suppported dialects and returns the first
    // supported instance.
    // Ideally we want to return the SQLUdfRepresentation that is
    // requested by the user in syntax

    // Created a sorted list of the dialects. DremioSQL should be the first dialect to be tried.
    List<String> sortedListOfSupportedDialectAsString =
        EnumSet.allOf(VersionedUdfMetadata.SupportedUdfDialects.class).stream()
            .map(Enum::toString)
            .sorted()
            .collect(Collectors.toList());
    Set<SQLUdfRepresentation> setOfSQLRepresentations = getSQLRepresentations();
    if (setOfSQLRepresentations.isEmpty()) {
      throw UserException.validationError()
          .message(
              "This udf contains unsupported  representations. Only SQL representations are currently supported. Check the udf metadata file for more details.")
          .build(logger);
    }
    // Find the first SQLViewRepresentation whose dialect matches any of the dialects in
    // sortedListOfSupportedDialectsForReadAsString in order.
    Optional<SQLUdfRepresentation> representationOpt =
        sortedListOfSupportedDialectAsString.stream()
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
            .map(SQLUdfRepresentation::dialect)
            .collect(Collectors.toList());

    List<String> supportedDialects =
        Arrays.stream(VersionedUdfMetadata.SupportedUdfDialects.values())
            .map(Enum::name)
            .collect(Collectors.toList());

    throw UserException.validationError()
        .message(
            "This udf contains unsupported SQL dialects. Supported dialects are: %s. Actual dialects in representations: %s",
            supportedDialects, actualDialects)
        .build(logger);
  }

  private SQLUdfRepresentation getValidSqlUdfRepresentation() {
    if (sqlUdfRepresentation != null) {
      return sqlUdfRepresentation;
    }
    validateAndGetSqlUdfRepresentation()
        .ifPresent(representation -> sqlUdfRepresentation = representation);
    return sqlUdfRepresentation;
  }
}
