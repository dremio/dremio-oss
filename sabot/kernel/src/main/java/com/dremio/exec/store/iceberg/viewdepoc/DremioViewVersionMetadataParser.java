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
package com.dremio.exec.store.iceberg.viewdepoc;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.JsonUtil;

/**
 * This class should be replaced into OSS Iceberg's ViewVersionParser. Most of the code in this
 * class is a copy from nessie-iceberg-view 0.73.0.
 */
public class DremioViewVersionMetadataParser {

  public static ViewVersionMetadata fromJson(String json) {
    Preconditions.checkArgument(json != null, "Cannot parse view metadata from null string");
    return JsonUtil.parse(json, DremioViewVersionMetadataParser::fromJson);
  }

  public static ViewVersionMetadata fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse metadata from a non-object: %s", node);
    int formatVersion = JsonUtil.getInt("format-version", node);
    Preconditions.checkArgument(
        formatVersion == 1, "Cannot read unsupported version %s", formatVersion);
    String location = JsonUtil.getString("location", node);
    int currentVersionId = JsonUtil.getInt("current-version-id", node);
    Map<String, String> properties = new HashMap();
    if (node.has("properties")) {
      properties.putAll(JsonUtil.getStringMap("properties", node));
    }

    JsonNode versionArray = node.get("versions");
    Preconditions.checkArgument(
        versionArray.isArray(), "Cannot parse versions from non-array: %s", versionArray);
    List<Version> versions = Lists.newArrayListWithExpectedSize(versionArray.size());
    Iterator<JsonNode> iterator = versionArray.elements();

    while (iterator.hasNext()) {
      versions.add(VersionParser.fromJson(String.valueOf((JsonNode) iterator.next())));
    }

    SortedSet<VersionLogEntry> entries =
        Sets.newTreeSet(Comparator.comparingLong(VersionLogEntry::timestampMillis));
    if (node.has("version-log")) {
      Iterator<JsonNode> logIterator = node.get("version-log").elements();

      while (logIterator.hasNext()) {
        JsonNode entryNode = (JsonNode) logIterator.next();
        entries.add(
            new VersionLogEntry(
                JsonUtil.getLong("timestamp-ms", entryNode),
                JsonUtil.getInt("version-id", entryNode)));
      }
    }

    if (((Version) versions.get(versions.size() - 1)).versionId() != currentVersionId) {
      throw new RuntimeIOException(
          "Version history is corrupt. Latest version info with id: %s does not match current version id: %s",
          new Object[] {
            ((Version) versions.get(versions.size() - 1)).versionId(), currentVersionId
          });
    } else {
      ViewDefinition viewMetadata = ((Version) versions.get(versions.size() - 1)).viewDefinition();
      return new ViewVersionMetadata(
          location,
          viewMetadata,
          properties,
          currentVersionId,
          versions,
          ImmutableList.copyOf(entries.iterator()));
    }
  }
}
