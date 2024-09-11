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

import com.dremio.exec.store.iceberg.dremioudf.api.udf.SQLUdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfHistoryEntry;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfRepresentation;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ImmutablesStyle")
@Value.Immutable(builder = false)
@Value.Style(allParameters = true, visibilityString = "PACKAGE")
public interface UdfMetadata extends Serializable {
  Logger LOG = LoggerFactory.getLogger(UdfMetadata.class);
  int SUPPORTED_UDF_FORMAT_VERSION = 0;
  int DEFAULT_UDF_FORMAT_VERSION = 0;

  String uuid();

  int formatVersion();

  String location();

  default String currentSignatureId() {
    String currentSignatureId = currentVersion().signatureId();
    Preconditions.checkArgument(
        signaturesById().containsKey(currentSignatureId),
        "Cannot find current signature with id %s in signatures: %s",
        currentSignatureId,
        signaturesById().keySet());

    return currentSignatureId;
  }

  String currentVersionId();

  List<UdfVersion> versions();

  List<UdfSignature> signatures();

  List<UdfHistoryEntry> history();

  Map<String, String> properties();

  List<MetadataUpdate> changes();

  @Nullable
  String metadataFileLocation();

  default UdfVersion version(String versionId) {
    return versionsById().get(versionId);
  }

  default UdfVersion currentVersion() {
    // fail when accessing the current version if UDFMetadata was created through the
    // UDFMetadataParser with an invalid UDF version id
    Preconditions.checkArgument(
        versionsById().containsKey(currentVersionId()),
        "Cannot find current version %s in UDF versions: %s",
        currentVersionId(),
        versionsById().keySet());

    return versionsById().get(currentVersionId());
  }

  @Value.Derived
  default Map<String, UdfVersion> versionsById() {
    ImmutableMap.Builder<String, UdfVersion> builder = ImmutableMap.builder();
    for (UdfVersion version : versions()) {
      builder.put(version.versionId(), version);
    }

    return builder.build();
  }

  @Value.Derived
  default Map<String, UdfSignature> signaturesById() {
    ImmutableMap.Builder<String, UdfSignature> builder = ImmutableMap.builder();
    for (UdfSignature signature : signatures()) {
      builder.put(signature.signatureId(), signature);
    }

    return builder.build();
  }

  @Value.Check
  default void check() {
    Preconditions.checkArgument(
        formatVersion() >= 0 && formatVersion() <= UdfMetadata.SUPPORTED_UDF_FORMAT_VERSION,
        "Unsupported format version: %s",
        formatVersion());
  }

  static Builder builder() {
    return new Builder();
  }

  static Builder buildFrom(UdfMetadata base) {
    return new Builder(base);
  }

  class Builder {
    private static final String LAST_ADDED = "";
    private final List<UdfVersion> versions;
    private final List<UdfSignature> signatures;
    private final List<UdfHistoryEntry> history;
    private final Map<String, String> properties;
    private final List<MetadataUpdate> changes;
    private int formatVersion = DEFAULT_UDF_FORMAT_VERSION;
    private String currentVersionId;
    private String location;
    private String uuid;
    private String metadataLocation;

    // internal change tracking
    private String lastAddedVersionId = null;
    private UdfHistoryEntry historyEntry = null;
    private UdfVersion previousUdfVersion = null;

    // indexes
    private final Map<String, UdfVersion> versionsById;
    private final Map<String, UdfSignature> signaturesById;

    private Builder() {
      this.versions = Lists.newArrayList();
      this.signatures = Lists.newArrayList();
      this.versionsById = Maps.newHashMap();
      this.signaturesById = Maps.newHashMap();
      this.history = Lists.newArrayList();
      this.properties = Maps.newHashMap();
      this.changes = Lists.newArrayList();
      this.uuid = null;
    }

    private Builder(UdfMetadata base) {
      this.versions = Lists.newArrayList(base.versions());
      this.versionsById = Maps.newHashMap(base.versionsById());
      this.signatures = Lists.newArrayList(base.signatures());
      this.signaturesById = Maps.newHashMap(base.signaturesById());
      this.history = Lists.newArrayList(base.history());
      this.properties = Maps.newHashMap(base.properties());
      this.changes = Lists.newArrayList();
      this.formatVersion = base.formatVersion();
      this.currentVersionId = base.currentVersionId();
      this.location = base.location();
      this.uuid = base.uuid();
      this.metadataLocation = null;
      this.previousUdfVersion = base.currentVersion();
    }

    public Builder upgradeFormatVersion(int newFormatVersion) {
      Preconditions.checkArgument(
          newFormatVersion >= formatVersion,
          "Cannot downgrade v%s UDF to v%s",
          formatVersion,
          newFormatVersion);

      if (formatVersion == newFormatVersion) {
        return this;
      }

      this.formatVersion = newFormatVersion;
      changes.add(new MetadataUpdate.UpgradeFormatVersion(newFormatVersion));
      return this;
    }

    public Builder setLocation(String newLocation) {
      Preconditions.checkArgument(null != newLocation, "Invalid location: null");
      if (null != location && location.equals(newLocation)) {
        return this;
      }

      this.location = newLocation;
      changes.add(new MetadataUpdate.SetLocation(newLocation));
      return this;
    }

    public Builder setMetadataLocation(String newMetadataLocation) {
      this.metadataLocation = newMetadataLocation;
      return this;
    }

    public Builder setCurrentVersionId(String newVersionId) {
      if (newVersionId.equals(LAST_ADDED)) {
        ValidationException.check(
            lastAddedVersionId != null,
            "Cannot set last version id: no current version id has been set");
        return setCurrentVersionId(lastAddedVersionId);
      }

      if (newVersionId.equals(currentVersionId)) {
        return this;
      }

      UdfVersion version = versionsById.get(newVersionId);
      Preconditions.checkArgument(
          version != null, "Cannot set current version to unknown version: %s", newVersionId);

      this.currentVersionId = newVersionId;

      if (lastAddedVersionId != null && lastAddedVersionId.equals(newVersionId)) {
        changes.add(new UdfMetadataUpdate.SetCurrentUdfVersion(LAST_ADDED));
      } else {
        changes.add(new UdfMetadataUpdate.SetCurrentUdfVersion(newVersionId));
      }

      return this;
    }

    public Builder setCurrentVersion(UdfVersion version, UdfSignature signature) {
      String newSignatureId = addSignatureInternal(signature);
      UdfVersion newVersion =
          ImmutableUdfVersion.builder().from(version).signatureId(newSignatureId).build();
      return setCurrentVersionId(addVersionInternal(newVersion));
    }

    public Builder addVersion(UdfVersion version) {
      addVersionInternal(version);
      return this;
    }

    private String addVersionInternal(UdfVersion newVersion) {
      String newVersionId = reuseOrCreateNewUDFVersionId(newVersion);
      UdfVersion version = newVersion;
      if (newVersionId != version.versionId()) {
        version = ImmutableUdfVersion.builder().from(version).versionId(newVersionId).build();
      }

      if (versionsById.containsKey(newVersionId)) {
        boolean addedInBuilder =
            changes(UdfMetadataUpdate.AddUdfVersion.class)
                .anyMatch(added -> added.udfVersion().versionId() == newVersionId);
        this.lastAddedVersionId = addedInBuilder ? newVersionId : null;
        return newVersionId;
      }

      Preconditions.checkArgument(
          signaturesById.containsKey(version.signatureId()),
          "Cannot add version with unknown signature: %s",
          version.signatureId());

      Set<String> dialects = Sets.newHashSet();
      for (UdfRepresentation repr : version.representations()) {
        if (repr instanceof SQLUdfRepresentation) {
          SQLUdfRepresentation sql = (SQLUdfRepresentation) repr;
          Preconditions.checkArgument(
              dialects.add(sql.dialect().toLowerCase(Locale.ROOT)),
              "Invalid UDF version: Cannot add multiple bodies for dialect %s",
              sql.dialect().toLowerCase(Locale.ROOT));
        }
      }

      version = ImmutableUdfVersion.builder().from(newVersion).build();

      versions.add(version);
      versionsById.put(version.versionId(), version);
      changes.add(new UdfMetadataUpdate.AddUdfVersion(version));
      history.add(
          ImmutableUdfHistoryEntry.builder()
              .timestampMillis(version.timestampMillis())
              .versionId(version.versionId())
              .build());

      this.lastAddedVersionId = newVersionId;

      return newVersionId;
    }

    private String reuseOrCreateNewUDFVersionId(UdfVersion udfVersion) {
      // if the UDF version already exists, use its id; otherwise generate a new one
      for (UdfVersion version : versions) {
        if (sameUDFVersion(version, udfVersion)) {
          return version.versionId();
        }
      }

      return StringUtils.isBlank(udfVersion.versionId())
          ? UdfUtil.generateUUID()
          : udfVersion.versionId();
    }

    /**
     * Checks whether the given UDF versions would behave the same while ignoring the UDF version
     * id, the creation timestamp, and the operation.
     *
     * @param one the UDF version to compare
     * @param two the UDF version to compare
     * @return true if the given UDF versions would behave the same
     */
    private boolean sameUDFVersion(UdfVersion one, UdfVersion two) {
      return Objects.equals(one.summary(), two.summary())
          && Objects.equals(one.representations(), two.representations())
          && Objects.equals(one.defaultCatalog(), two.defaultCatalog())
          && Objects.equals(one.defaultNamespace(), two.defaultNamespace());
    }

    public Builder addSignature(UdfSignature signature) {
      addSignatureInternal(signature);
      return this;
    }

    private String addSignatureInternal(UdfSignature signature) {
      String newSignatureId = reuseOrCreateNewSignatureId(signature);
      if (signaturesById.containsKey(newSignatureId)) {
        // this signature existed or was already added in the builder
        return newSignatureId;
      }

      UdfSignature newSignature;
      if (!newSignatureId.equals(signature.signatureId())) {
        newSignature =
            ImmutableUdfSignature.builder()
                .signatureId(newSignatureId)
                .addAllParameters(signature.parameters())
                .returnType(signature.returnType())
                .deterministic(signature.deterministic())
                .build();
      } else {
        newSignature = signature;
      }

      signatures.add(newSignature);
      signaturesById.put(newSignature.signatureId(), newSignature);
      changes.add(new UdfMetadataUpdate.AddSignature(newSignature));

      return newSignatureId;
    }

    private String reuseOrCreateNewSignatureId(UdfSignature newSignature) {
      // if the schema already exists, use its id; otherwise generate a new one
      for (UdfSignature signature : signatures) {
        if (signature.sameSignature(newSignature)) {
          return signature.signatureId();
        }
      }

      return StringUtils.isBlank(newSignature.signatureId())
          ? UdfUtil.generateUUID()
          : newSignature.signatureId();
    }

    public Builder setProperties(Map<String, String> updated) {
      if (updated.isEmpty()) {
        return this;
      }

      properties.putAll(updated);
      changes.add(new MetadataUpdate.SetProperties(updated));
      return this;
    }

    public Builder removeProperties(Set<String> propertiesToRemove) {
      if (propertiesToRemove.isEmpty()) {
        return this;
      }

      propertiesToRemove.forEach(properties::remove);
      changes.add(new MetadataUpdate.RemoveProperties(propertiesToRemove));
      return this;
    }

    public UdfMetadata.Builder assignUUID(String newUUID) {
      Preconditions.checkArgument(newUUID != null, "Cannot set uuid to null");
      Preconditions.checkArgument(uuid == null || newUUID.equals(uuid), "Cannot reassign uuid");

      if (!newUUID.equals(uuid)) {
        this.uuid = newUUID;
        changes.add(new MetadataUpdate.AssignUUID(uuid));
      }

      return this;
    }

    public UdfMetadata build() {
      Preconditions.checkArgument(null != location, "Invalid location: null");
      Preconditions.checkArgument(!versions.isEmpty(), "Invalid UDF: no versions were added");

      // when associated with a metadata file, metadata must have no changes so that the metadata
      // matches exactly what is in the metadata file, which does not store changes. metadata
      // location with changes is inconsistent.
      Preconditions.checkArgument(
          metadataLocation == null || changes.isEmpty(),
          "Cannot create UDF metadata with a metadata location and changes");

      if (null != historyEntry) {
        history.add(historyEntry);
      }

      if (null != previousUdfVersion
          && !PropertyUtil.propertyAsBoolean(
              properties,
              UdfProperties.REPLACE_DROP_DIALECT_ALLOWED,
              UdfProperties.REPLACE_DROP_DIALECT_ALLOWED_DEFAULT)) {
        checkIfDialectIsDropped(previousUdfVersion, versionsById.get(currentVersionId));
      }

      int historySize =
          PropertyUtil.propertyAsInt(
              properties,
              UdfProperties.VERSION_HISTORY_SIZE,
              UdfProperties.VERSION_HISTORY_SIZE_DEFAULT);

      Preconditions.checkArgument(
          historySize > 0,
          "%s must be positive but was %s",
          UdfProperties.VERSION_HISTORY_SIZE,
          historySize);

      // expire old versions, but keep at least the versions added in this builder
      int numAddedVersions = (int) changes(UdfMetadataUpdate.AddUdfVersion.class).count();
      int numVersionsToKeep = Math.max(numAddedVersions, historySize);

      List<UdfVersion> retainedVersions;
      List<UdfHistoryEntry> retainedHistory;
      if (versions.size() > numVersionsToKeep) {
        retainedVersions = expireVersions(versionsById, numVersionsToKeep);
        Set<String> retainedVersionIds =
            retainedVersions.stream().map(UdfVersion::versionId).collect(Collectors.toSet());
        retainedHistory = updateHistory(history, retainedVersionIds);
      } else {
        retainedVersions = versions;
        retainedHistory = history;
      }

      // keep all signatures referred by retained versions
      Set<UdfSignature> retainedSignatures = Sets.newHashSet();
      for (UdfVersion version : retainedVersions) {
        UdfSignature signature = signaturesById.getOrDefault(version.signatureId(), null);
        if (signature != null) {
          retainedSignatures.add(signature);
        }
      }

      return ImmutableUdfMetadata.of(
          null == uuid ? UdfUtil.generateUUID() : uuid,
          formatVersion,
          location,
          currentVersionId,
          retainedVersions,
          retainedSignatures,
          retainedHistory,
          properties,
          changes,
          metadataLocation);
    }

    @VisibleForTesting
    static List<UdfVersion> expireVersions(
        Map<String, UdfVersion> versionsById, int numVersionsToKeep) {
      // version ids are assigned sequentially. keep the latest versions by ID.
      List<String> ids = Lists.newArrayList(versionsById.keySet());
      ids.sort(Comparator.reverseOrder());

      List<UdfVersion> retainedVersions = Lists.newArrayList();
      for (String idToKeep : ids.subList(0, numVersionsToKeep)) {
        retainedVersions.add(versionsById.get(idToKeep));
      }

      return retainedVersions;
    }

    @VisibleForTesting
    static List<UdfHistoryEntry> updateHistory(List<UdfHistoryEntry> history, Set<String> ids) {
      List<UdfHistoryEntry> retainedHistory = Lists.newArrayList();
      for (UdfHistoryEntry entry : history) {
        if (ids.contains(entry.versionId())) {
          retainedHistory.add(entry);
        } else {
          // clear history past any unknown version
          retainedHistory.clear();
        }
      }

      return retainedHistory;
    }

    private <U extends MetadataUpdate> Stream<U> changes(Class<U> updateClass) {
      return changes.stream().filter(updateClass::isInstance).map(updateClass::cast);
    }

    private void checkIfDialectIsDropped(UdfVersion previous, UdfVersion current) {
      Set<String> baseDialects = sqlDialectsFor(previous);
      Set<String> updatedDialects = sqlDialectsFor(current);

      Preconditions.checkState(
          updatedDialects.containsAll(baseDialects),
          "Cannot replace UDF due to loss of UDF dialects (%s=false):\nPrevious dialects: %s\nNew dialects: %s",
          UdfProperties.REPLACE_DROP_DIALECT_ALLOWED,
          baseDialects,
          updatedDialects);
    }

    private Set<String> sqlDialectsFor(UdfVersion udfVersion) {
      Set<String> dialects = Sets.newHashSet();
      for (UdfRepresentation repr : udfVersion.representations()) {
        if (repr instanceof SQLUdfRepresentation) {
          SQLUdfRepresentation sql = (SQLUdfRepresentation) repr;
          dialects.add(sql.dialect().toLowerCase(Locale.ROOT));
        }
      }

      return dialects;
    }
  }
}
