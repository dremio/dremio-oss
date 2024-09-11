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
package com.dremio.dac.api;

import com.dremio.catalog.model.VersionContext;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@JsonDeserialize(as = ImmutableCatalogPageToken.class)
@JsonSerialize(as = ImmutableCatalogPageToken.class)
public abstract class CatalogPageToken {
  private static final Logger logger = LoggerFactory.getLogger(CatalogPageToken.class);

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper()
          .setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);
  private static final Base64.Decoder BASE64_DECODER = Base64.getUrlDecoder();
  private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

  /**
   * Git-like ref of the catalog that was passed in the previous call. This is used for validation
   * of the ref parameter.
   */
  @Nullable
  public abstract VersionContext versionContext();

  /**
   * Path of the item being listed, the path is dot-separated. This is used for validation of the
   * path parameter on subsequent calls.
   */
  public abstract String path();

  /** Name of the child item to start listing from in KV store or Nessie pageToken. */
  public abstract String pageToken();

  /** Immutables toBuilder. */
  public ImmutableCatalogPageToken.Builder toBuilder() {
    return new ImmutableCatalogPageToken.Builder().from(this);
  }

  /** Immutables builder. */
  public static ImmutableCatalogPageToken.Builder builder() {
    return new ImmutableCatalogPageToken.Builder();
  }

  /** Parses base64 url encoded JSON of PaginationToken. */
  public static CatalogPageToken fromApiToken(String paginationToken) {
    try {
      return OBJECT_MAPPER.readValue(
          BASE64_DECODER.decode(paginationToken), CatalogPageToken.class);
    } catch (IOException e) {
      logger.error("Failed to parse pagination token: {}", paginationToken, e);
      throw new IllegalArgumentException("Failed to parse pagination token.", e);
    }
  }

  /** Returns base64 url encoded JSON of this. */
  public String toApiToken() {
    try {
      return BASE64_ENCODER.encodeToString(OBJECT_MAPPER.writeValueAsBytes(this));
    } catch (JsonProcessingException e) {
      logger.error("Failed to serialize pagination token", e);
      throw new RuntimeException(e);
    }
  }

  /** Given path to the last child, stores dot-separated path to the parent and child name. */
  public static CatalogPageToken fromStartChild(List<String> path) {
    return new ImmutableCatalogPageToken.Builder()
        .setPageToken(path.get(path.size() - 1))
        .setPath(pathToString(path.subList(0, path.size() - 1)))
        .build();
  }

  /** For versioned sources (i.e. Nessie). */
  public static CatalogPageToken fromPathAndVersion(
      List<String> parentPath, VersionContext versionContext, String pageToken) {
    return new ImmutableCatalogPageToken.Builder()
        .setPageToken(pageToken)
        .setVersionContext(versionContext)
        .setPath(pathToString(parentPath))
        .build();
  }

  /**
   * Converts list of path components to a dot-separated string. If a component has double quote,
   * that quote is doubled. If a component has a double quote or dot, that component is
   * double-quoted.
   */
  public static String pathToString(List<String> path) {
    return path.stream()
        .map(CatalogPageToken::escapePathComponent)
        .collect(Collectors.joining("."));
  }

  /** Parses dot-separated, escaped string into path components. */
  public static List<String> stringToPath(String escaped) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    if (!Strings.isNullOrEmpty(escaped)) {
      int start = 0;
      int end = 0;
      boolean startQuoted = escaped.charAt(start) == '\"';
      do {
        boolean endOfComponent = false;
        if (end >= escaped.length()) {
          if (startQuoted) {
            throw new RuntimeException(
                String.format("Missing double quote at %d in '%s'", end + 1, escaped));
          }
          endOfComponent = true;
        } else {
          char nextChar = escaped.charAt(end);
          if (startQuoted) {
            // Component must have a closing quote.
            if (nextChar == '"' && end > start) {
              if (end + 1 >= escaped.length()) {
                endOfComponent = true;
              } else {
                char nextNextChar = escaped.charAt(end + 1);
                if (nextNextChar == '"') {
                  // Escaped double quote, skip it.
                  end++;
                } else if (nextNextChar != '.') {
                  throw new RuntimeException(
                      String.format("Char at %d in '%s' must be a dot", end + 2, escaped));
                } else {
                  endOfComponent = true;
                }
              }
            }
          } else {
            // Must end with dot.
            endOfComponent = nextChar == '.';
          }
        }

        // Add new component.
        if (endOfComponent) {
          if (end < escaped.length() && escaped.charAt(end) == '\"') {
            end++;
          }
          builder.add(
              unescapePathComponent(escaped.substring(start, Math.min(escaped.length(), end))));
          if (end < escaped.length() && escaped.charAt(end) == '.') {
            end++;
          }
          start = end;
          startQuoted = start < escaped.length() && escaped.charAt(start) == '\"';
        }

        end++;
      } while (end <= escaped.length());
    }
    return builder.build();
  }

  private static String escapePathComponent(String name) {
    boolean doubleQuote = name.contains(".");
    if (name.contains("\"")) {
      name = name.replace("\"", "\"\"");
      doubleQuote = true;
    }
    if (doubleQuote) {
      name = "\"" + name + "\"";
    }
    return name;
  }

  private static String unescapePathComponent(String name) {
    if (name.startsWith("\"")) {
      if (!name.endsWith("\"")) {
        throw new IllegalArgumentException(
            String.format("Escaped name '%s' does not end with double quote", name));
      }
      name = name.substring(1, name.length() - 1);
    }
    return name.replace("\"\"", "\"");
  }
}
