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
package com.dremio.exec.catalog.dataplane.test;

import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.METADATA_FOLDER;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.USER_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.common.utils.PathUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.projectnessie.model.UDF;

/** Dataplane test assertion helpers */
public final class TestDataplaneAssertions {

  private TestDataplaneAssertions() {}

  public static void assertNessieHasCommitForTable(
      List<String> tableSchemaComponents,
      Class<? extends Operation> operationType,
      String branchName,
      DataplaneTestHelper base)
      throws NessieNotFoundException {
    final List<LogResponse.LogEntry> logEntries =
        base.getNessieApi()
            .getCommitLog()
            .refName(branchName)
            .fetch(FetchOption.ALL) // Get extended data, including operations
            .get()
            .getLogEntries();
    assertThat(logEntries).hasSizeGreaterThanOrEqualTo(1);
    final LogResponse.LogEntry mostRecentLogEntry =
        logEntries.get(0); // Commits are ordered most recent to earliest

    final List<Operation> operations = mostRecentLogEntry.getOperations();
    assertThat(operations).hasSizeGreaterThanOrEqualTo(1);
    // Taking the last operation in the commit because for testing, this is the one that will
    // contain the entire tableschemapath.
    final Operation operation = operations.get(operations.size() - 1);
    assertThat(operationType).isAssignableFrom(operation.getClass());

    final ContentKey actualContentKey = operation.getKey();
    final ContentKey expectedContentKey = ContentKey.of(tableSchemaComponents);
    assertThat(actualContentKey).isEqualTo(expectedContentKey);
  }

  public static void assertNessieHasTable(
      List<String> tableSchemaComponents, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi()
            .getContent()
            .refName(branchName)
            .key(ContentKey.of(tableSchemaComponents))
            .get();

    ContentKey expectedContentsKey = ContentKey.of(tableSchemaComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    Optional<IcebergTable> maybeIcebergTable =
        contentsMap.get(expectedContentsKey).unwrap(IcebergTable.class);
    assertThat(maybeIcebergTable).isPresent();
    assertThat(
            base.getDataplaneStorage()
                .doesObjectExist(PRIMARY_BUCKET, maybeIcebergTable.get().getMetadataLocation()))
        .isTrue();
  }

  public static void assertNessieHasView(
      List<String> viewSchemaComponents, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {
    Reference branch = base.getNessieApi().getReference().refName(branchName).get();
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi()
            .getContent()
            .reference(branch)
            .key(ContentKey.of(viewSchemaComponents))
            .get();

    ContentKey expectedContentsKey = ContentKey.of(viewSchemaComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    Optional<IcebergView> maybeIcebergView =
        contentsMap.get(expectedContentsKey).unwrap(IcebergView.class);
    assertThat(maybeIcebergView).isPresent();
    assertThat(
            base.getDataplaneStorage()
                .doesObjectExist(PRIMARY_BUCKET, maybeIcebergView.get().getMetadataLocation()))
        .isTrue();
  }

  public static void assertNessieHasFunction(
      List<String> schemaComponents, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {
    Reference branch = base.getNessieApi().getReference().refName(branchName).get();
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi()
            .getContent()
            .reference(branch)
            .key(ContentKey.of(schemaComponents))
            .get();

    ContentKey expectedContentsKey = ContentKey.of(schemaComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    Optional<UDF> maybeUdf = contentsMap.get(expectedContentsKey).unwrap(UDF.class);
    assertThat(maybeUdf).isPresent();
    assertThat(
            base.getDataplaneStorage()
                .doesObjectExist(PRIMARY_BUCKET, maybeUdf.get().getMetadataLocation()))
        .isTrue();
  }

  public static void assertNessieHasNamespace(
      List<String> namespaceComponents, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {

    Reference branch = base.getNessieApi().getReference().refName(branchName).get();
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi()
            .getContent()
            .reference(branch)
            .key(ContentKey.of(namespaceComponents))
            .get();

    ContentKey expectedContentsKey = ContentKey.of(namespaceComponents);
    assertThat(contentsMap).containsKey(expectedContentsKey);

    Optional<Namespace> namespace = contentsMap.get(expectedContentsKey).unwrap(Namespace.class);
    assertThat(namespace).isPresent();
  }

  public static void assertNessieDoesNotHaveNamespace(
      List<String> namespaceComponents, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {

    Reference branch = base.getNessieApi().getReference().refName(branchName).get();
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi()
            .getContent()
            .reference(branch)
            .key(ContentKey.of(namespaceComponents))
            .get();

    ContentKey expectedContentsKey = ContentKey.of(namespaceComponents);
    assertThat(contentsMap).doesNotContainKey(expectedContentsKey);
  }

  public static void assertLastCommitMadeBySpecifiedAuthor(
      String branchName, DataplaneTestHelper base) throws NessieNotFoundException {
    final List<LogResponse.LogEntry> logEntries =
        base.getNessieApi()
            .getCommitLog()
            .refName(branchName)
            .fetch(FetchOption.ALL) // Get extended data, including operations
            .get()
            .getLogEntries();
    assertThat(logEntries).hasSizeGreaterThanOrEqualTo(1);
    final LogResponse.LogEntry mostRecentLogEntry =
        logEntries.get(0); // Commits are ordered most recent to earliest

    final List<Operation> operations = mostRecentLogEntry.getOperations();
    assertThat(operations).hasSizeGreaterThanOrEqualTo(1);
    assertThat(mostRecentLogEntry.getCommitMeta().getAuthor()).isEqualTo(USER_NAME);
  }

  public static void assertNessieDoesNotHaveEntity(
      List<String> key, String branchName, DataplaneTestHelper base)
      throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi().getContent().refName(branchName).key(ContentKey.of(key)).get();
    assertThat(contentsMap).isEmpty();
  }

  public static List<String> getSubPathFromNessieTableContent(
      List<String> tablePath, String refName, DataplaneTestHelper base)
      throws NessieNotFoundException {
    Map<ContentKey, Content> contentsMap =
        base.getNessieApi().getContent().refName(refName).key(ContentKey.of(tablePath)).get();
    ContentKey contentKey = ContentKey.of(tablePath);
    Optional<IcebergTable> maybeIcebergTable =
        contentsMap.get(contentKey).unwrap(IcebergTable.class);

    List<String> pathComponents =
        PathUtils.toPathComponents(maybeIcebergTable.get().getMetadataLocation());
    int metadataIndex = pathComponents.indexOf(METADATA_FOLDER);
    int beginningKeyIndex =
        IntStream.range(0, pathComponents.size())
            .filter(i -> pathComponents.get(i).startsWith(tablePath.get(0)))
            .findFirst()
            .orElse(-1);
    if (beginningKeyIndex != -1 && (metadataIndex > 0) && (metadataIndex < pathComponents.size())) {
      return pathComponents.subList(beginningKeyIndex, metadataIndex);
    } else {
      return Collections.emptyList();
    }
  }

  public static void assertIcebergTableExistsAtSubPath(
      List<String> subPath, DataplaneTestHelper base) {
    // Iceberg tables on disk have a "metadata" folder in their root, check for "metadata" folder
    // too
    List<String> pathToMetadataFolder = new ArrayList<>(subPath);
    pathToMetadataFolder.add(METADATA_FOLDER);

    List<String> keysInMetadataSubPath =
        base.getDataplaneStorage()
            .listObjectNames(PRIMARY_BUCKET, String.join("/", pathToMetadataFolder))
            .collect(Collectors.toList());

    assertThat(keysInMetadataSubPath.size()).isGreaterThanOrEqualTo(1);
  }

  public static void assertIcebergFilesExistAtSubPath(
      List<String> subPath,
      int expectedNumAvroFilesExcludingSnapshot,
      int expectedNumMetadataJsonFiles,
      int expectedNumSnapshotFiles,
      int expectedNumParquetFiles,
      DataplaneTestHelper base) {
    List<String> pathToMetadataFolder = new ArrayList<>(subPath);
    pathToMetadataFolder.add(METADATA_FOLDER);

    List<String> keysInMetadataSubPath =
        base.getDataplaneStorage()
            .listObjectNames(PRIMARY_BUCKET, String.join("/", pathToMetadataFolder))
            .collect(Collectors.toList());

    // check for avro files
    assertThat(
            keysInMetadataSubPath.stream()
                .filter(key -> key.endsWith(".avro") && !(key.contains("snap")))
                .count())
        .isEqualTo(expectedNumAvroFilesExcludingSnapshot);

    // check for snapshot files
    assertThat(keysInMetadataSubPath.stream().filter(key -> key.contains("snap")).count())
        .isEqualTo(expectedNumSnapshotFiles);

    // Check for metadata.json file
    assertThat(keysInMetadataSubPath.stream().filter(key -> key.endsWith(".metadata.json")).count())
        .isEqualTo(expectedNumMetadataJsonFiles);

    List<String> keysInSubPath =
        base.getDataplaneStorage()
            .listObjectNames(PRIMARY_BUCKET, String.join("/", subPath))
            .collect(Collectors.toList());

    // Check for .parquet files
    assertThat(keysInSubPath.stream().filter(key -> key.endsWith(".parquet")).count())
        .isEqualTo(expectedNumParquetFiles);
  }

  public static void assertNessieDoesHotHaveBranch(String branchName, DataplaneTestHelper base) {
    try {
      Reference branch = base.getNessieApi().getReference().refName(branchName).get();
      // this will always throw.
      assertThat(branch).isNull();
    } catch (NessieNotFoundException e) {
      // Intentionally left blank.
    }
  }

  public static void assertNessieDoesHotHaveTag(String tagName, DataplaneTestHelper base) {
    try {
      Reference tag = base.getNessieApi().getReference().refName(tagName).get();
      // this will always throw.
      assertThat(tag).isNull();
    } catch (NessieNotFoundException e) {
      // Intentionally left blank.
    }
  }
}
