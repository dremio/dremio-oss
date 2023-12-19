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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.METADATA_FOLDER;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.S3_PREFIX;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.USER_NAME;
import static com.dremio.exec.catalog.dataplane.ITDataplanePluginTestSetup.getS3Client;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

import com.amazonaws.services.s3.model.S3ObjectSummary;


/**
 * Dataplane test assertion helpers
 */
public final class TestDataplaneAssertions {

  private TestDataplaneAssertions() {
  }

  public static void assertNessieHasCommitForTable(List<String> tableSchemaComponents,
                                               Class<? extends Operation> operationType,
                                               String branchName,
                                               ITDataplanePluginTestSetup base
  ) throws NessieNotFoundException {
    final List<LogResponse.LogEntry> logEntries = base.getNessieClient()
      .getCommitLog()
      .refName(branchName)
      .fetch(FetchOption.ALL) // Get extended data, including operations
      .get()
      .getLogEntries();
    assertThat(logEntries).hasSizeGreaterThanOrEqualTo(1);
    final LogResponse.LogEntry mostRecentLogEntry = logEntries.get(0); // Commits are ordered most recent to earliest

    final List<Operation> operations = mostRecentLogEntry.getOperations();
    assertThat(operations).hasSize(1);
    final Operation operation = operations.get(0);
    assertThat(operationType).isAssignableFrom(operation.getClass());

    final ContentKey actualContentKey = operation.getKey();
    final ContentKey expectedContentKey = ContentKey.of(tableSchemaComponents);
    assertThat(actualContentKey).isEqualTo(expectedContentKey);
  }

  public static void assertNessieHasTable(List<String> tableSchemaComponents,
                                          String branchName,
                                          ITDataplanePluginTestSetup base) throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .refName(branchName)
      .key(ContentKey.of(tableSchemaComponents))
      .get();

    ContentKey expectedContentsKey = ContentKey.of(tableSchemaComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    String expectedMetadataLocationPrefix = S3_PREFIX + BUCKET_NAME + "/" + String.join("/", tableSchemaComponents) + "/" + METADATA_FOLDER;
    Optional<IcebergTable> maybeIcebergTable = contentsMap
      .get(expectedContentsKey)
      .unwrap(IcebergTable.class);
    assertThat(maybeIcebergTable).isPresent();
    assertThat(maybeIcebergTable.get().getMetadataLocation()).startsWith(expectedMetadataLocationPrefix);
  }

  public static void assertNessieHasView(List<String> viewSchemaComponents,
                                         String branchName,
                                         ITDataplanePluginTestSetup base) throws NessieNotFoundException {
    Reference branch = base.getNessieClient().getReference()
      .refName(branchName)
      .get();
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .reference(branch)
      .key(ContentKey.of(viewSchemaComponents))
      .get();


    ContentKey expectedContentsKey = ContentKey.of(viewSchemaComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    String expectedMetadataLocationPrefix = S3_PREFIX + BUCKET_NAME + "/" + String.join("/", viewSchemaComponents) + "/" + METADATA_FOLDER;
    Optional<IcebergView> icebergView = contentsMap
      .get(expectedContentsKey)
      .unwrap(IcebergView.class);
    assertThat(icebergView).isPresent();
    assertThat(icebergView.get().getMetadataLocation()).startsWith(expectedMetadataLocationPrefix);
  }

  public static void assertNessieHasNamespace(List<String> namespaceComponents,
                                              String branchName,
                                              ITDataplanePluginTestSetup base) throws NessieNotFoundException {

    Reference branch = base.getNessieClient().getReference()
      .refName(branchName)
      .get();
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .reference(branch)
      .key(ContentKey.of(namespaceComponents))
      .get();

    ContentKey expectedContentsKey = ContentKey.of(namespaceComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    Optional<Namespace> namespace = contentsMap
      .get(expectedContentsKey)
      .unwrap(Namespace.class);
    assertThat(namespace).isPresent();
  }

  public static void assertNessieDoesNotHaveNamespace(List<String> namespaceComponents,
                                              String branchName,
                                              ITDataplanePluginTestSetup base) throws NessieNotFoundException {

    Reference branch = base.getNessieClient().getReference()
      .refName(branchName)
      .get();
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .reference(branch)
      .key(ContentKey.of(namespaceComponents))
      .get();

    ContentKey expectedContentsKey = ContentKey.of(namespaceComponents);
    assertThat(contentsMap).doesNotContainKey(expectedContentsKey);
  }

  public static void assertLastCommitMadeBySpecifiedAuthor(String branchName,
                                                           ITDataplanePluginTestSetup base) throws NessieNotFoundException {
    final List<LogResponse.LogEntry> logEntries = base.getNessieClient()
      .getCommitLog()
      .refName(branchName)
      .fetch(FetchOption.ALL) // Get extended data, including operations
      .get()
      .getLogEntries();
    assertThat(logEntries).hasSizeGreaterThanOrEqualTo(1);
    final LogResponse.LogEntry mostRecentLogEntry = logEntries.get(0); // Commits are ordered most recent to earliest

    final List<Operation> operations = mostRecentLogEntry.getOperations();
    assertThat(operations).hasSize(1);
    assertThat(mostRecentLogEntry.getCommitMeta().getAuthor()).isEqualTo(USER_NAME);
  }

  public static void assertNessieDoesNotHaveView(List<String> viewKey,
                                                 String branchName,
                                                 ITDataplanePluginTestSetup base) throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .refName(branchName)
      .key(ContentKey.of(viewKey))
      .get();
    assertThat(contentsMap).isEmpty();
  }

  public static void assertNessieDoesNotHaveTable(List<String> tableSchemaComponents,
                                                  String branchName,
                                                  ITDataplanePluginTestSetup base) throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap = base.getNessieClient()
      .getContent()
      .refName(branchName)
      .key(ContentKey.of(tableSchemaComponents))
      .get();
    assertThat(contentsMap).isEmpty();
  }

  public static void assertIcebergTableExistsAtSubPath(List<String> subPath) {
    // Iceberg tables on disk have a "metadata" folder in their root, check for "metadata" folder too
    List<String> pathToMetadataFolder = new ArrayList<>(subPath);
    pathToMetadataFolder.add(METADATA_FOLDER);

    List<String> keysInMetadataSubPath = getS3Client().listObjects(BUCKET_NAME, String.join("/", pathToMetadataFolder)).getObjectSummaries().stream()
      .map(S3ObjectSummary::getKey)
      .collect(Collectors.toList());

    assertThat(keysInMetadataSubPath.size()).isGreaterThanOrEqualTo(1);
  }

  public static void assertIcebergFilesExistAtSubPath(List<String> subPath,
                                               int expectedNumAvroFilesExcludingSnapshot,
                                               int expectedNumMetadataJsonFiles,
                                               int expectedNumSnapshotFiles,
                                               int expectedNumParquetFiles) {
    List<String> pathToMetadataFolder = new ArrayList<>(subPath);
    pathToMetadataFolder.add(METADATA_FOLDER);

    List<String> keysInMetadataSubPath = getS3Client().listObjects(BUCKET_NAME, String.join("/", pathToMetadataFolder)).getObjectSummaries().stream()
      .map(S3ObjectSummary::getKey)
      .collect(Collectors.toList());

    //check for avro files
    assertThat(keysInMetadataSubPath.stream()
      .filter(key -> key.endsWith(".avro") && !(key.contains("snap")))
      .collect(Collectors.toList())
      .size()
    ).isEqualTo(expectedNumAvroFilesExcludingSnapshot);

    //check for snapshot files
    assertThat(keysInMetadataSubPath.stream()
      .filter(key -> key.contains("snap"))
      .collect(Collectors.toList())
      .size()
    ).isEqualTo(expectedNumSnapshotFiles);

    //Check for metadata.json file
    assertThat(keysInMetadataSubPath.stream()
      .filter(key -> key.endsWith(".metadata.json"))
      .collect(Collectors.toList())
      .size()
    ).isEqualTo(expectedNumMetadataJsonFiles);

    List<String> keysInSubPath = getS3Client().listObjects(BUCKET_NAME, String.join("/", subPath)).getObjectSummaries().stream()
      .map(S3ObjectSummary::getKey)
      .collect(Collectors.toList());

    //Check for .parquet files
    assertThat(keysInSubPath.stream()
      .filter(key -> key.endsWith(".parquet"))
      .collect(Collectors.toList())
      .size()
    ).isEqualTo(expectedNumParquetFiles);
  }

  public static void assertNessieDoesHotHaveBranch(String branchName, ITDataplanePluginTestSetup base) {
    try {
      Reference branch = base.getNessieClient().getReference()
        .refName(branchName)
        .get();
      //this will always throw.
      assertThat(branch).isNull();
    } catch (NessieNotFoundException e) {
      // Intentionally left blank.
    }
  }

  public static void assertNessieDoesHotHaveTag(String tagName, ITDataplanePluginTestSetup base) {
    try {
      Reference tag = base.getNessieClient().getReference()
        .refName(tagName)
        .get();
      //this will always throw.
      assertThat(tag).isNull();
    } catch (NessieNotFoundException e) {
      // Intentionally left blank.
    }
  }
}
