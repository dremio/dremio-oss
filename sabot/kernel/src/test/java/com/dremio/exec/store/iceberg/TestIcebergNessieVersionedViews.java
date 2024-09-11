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

import static com.dremio.plugins.NessieClientImpl.DUMMY_NESSIE_DIALECT;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.iceberg.viewdepoc.View;
import com.dremio.exec.store.iceberg.viewdepoc.ViewDefinition;
import com.dremio.plugins.NessieContent;
import com.google.common.base.Joiner;
import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewVersion;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

public class TestIcebergNessieVersionedViews extends BaseIcebergViewTest {
  private static final String SQL = "select id from tb1";
  private static final Schema SCHEMA =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final String NEW_SQL = "select name from tb1";
  private static final Schema NEW_SCHEMA =
      new Schema(Types.StructType.of(required(1, "name", Types.StringType.get())).fields());
  private static final String CATALOG_NAME = "nessie";
  private static final String DB_NAME = "db";
  private static final TableIdentifier VIEW_IDENTIFIER =
      TableIdentifier.of(CATALOG_NAME, DB_NAME, "view_name");

  private static final String MAIN_BRANCH = "main";
  private static final String CREATE_BRANCH = "create_branch";
  private static final String REPLACE_BRANCH = "replace_branch";
  private static final String DROP_BRANCH = "drop_branch";
  private static final String COMPLEX_DROP_BRANCH = "complex_drop_branch";
  private static final String OLD_DROP_BRANCH = "old_drop_branch";
  private static final String NEW_DROP_BRANCH = "new_drop_branch";
  private static final String GLOBAL_METADATA_BRANCH = "global_metadata_branch";
  private static final String GLOBAL_NEW_METADATA_BRANCH = "global_new_metadata_branch";
  private static final String NON_GLOBAL_METADATA_BRANCH = "non_global_metadata_branch";
  private static final String NEW_NON_GLOBAL_METADATA_BRANCH = "new_non_global_metadata_branch";
  private static final String DIALECT_BRANCH = "dialect_branch_v0";
  private static final String DIALECT_BRANCH_V1 = "dialect_branch_v1";

  private static final List<String> createViewKey = Arrays.asList("create", "foo", "bar");
  private static final List<String> dialectViewKeyV0 = Arrays.asList("dialectV0", "foo", "bar");
  private static final List<String> dialectViewKeyV1 = Arrays.asList("dialectV1", "foo", "bar");
  private static final List<String> replaceViewKey = Arrays.asList("replace", "foo", "bar");
  private static final List<String> dropViewKey = Arrays.asList("drop", "foo", "bar");
  private static final List<String> complexDropViewKey =
      Arrays.asList("complex_drop", "foo", "bar");
  private static final List<String> newDropViewKey = Arrays.asList("new_drop", "foo", "bar");
  private static final List<String> nonExistViewKey = Arrays.asList("nonexist", "foo", "bar");
  private static final List<String> existViewKey = Arrays.asList("exist", "foo", "bar");
  private static final List<String> nonGlobalMetadataViewKey =
      Arrays.asList("non_global_metadata", "foo", "bar");

  private NessieContent fetchContent(List<String> key, String branch) {
    ResolvedVersionContext versionContext = getVersion(branch);
    Optional<NessieContent> nessieContent = nessieClient.getContent(key, versionContext, null);
    assertThat(nessieContent).isPresent();
    return nessieContent.get();
  }

  @Test
  public void testCreateView() {
    createBranch(CREATE_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(CREATE_BRANCH, ContentKey.of(createViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        createViewKey, viewDefinition, Collections.emptyMap(), getVersion(CREATE_BRANCH), s -> s);

    final View icebergView =
        icebergNessieVersionedViewsV0.load(createViewKey, getVersion(CREATE_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);
    assertThat(Paths.get(getViewPath(createViewKey))).exists();
    assertThat(getViewMetadatas(createViewKey)).isNotNull().hasSize(1);

    final ViewDefinition icebergViewDefinition =
        icebergNessieVersionedViewsV0.loadDefinition(createViewKey, getVersion(CREATE_BRANCH));
    assertThat(icebergViewDefinition).isEqualTo(viewDefinition);
  }

  @Test
  public void testReplaceView() {
    createBranch(REPLACE_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(REPLACE_BRANCH, ContentKey.of(replaceViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        replaceViewKey, viewDefinition, Collections.emptyMap(), getVersion(REPLACE_BRANCH), s -> s);

    final View icebergView =
        icebergNessieVersionedViewsV0.load(replaceViewKey, getVersion(REPLACE_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, NEW_SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.replace(
        replaceViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(REPLACE_BRANCH),
        s -> s);

    final View newIcebergView =
        icebergNessieVersionedViewsV0.load(replaceViewKey, getVersion(REPLACE_BRANCH));

    assertThat(newIcebergView).isNotNull();
    assertThat(newIcebergView.currentVersion().versionId()).isEqualTo(2);
    assertThat(newIcebergView.currentVersion().parentId()).isEqualTo(1);
    assertThat(newIcebergView.currentVersion().viewDefinition()).isEqualTo(newViewDefinition);
    assertThat(Paths.get(getViewPath(replaceViewKey))).exists();
    assertThat(getViewMetadatas(replaceViewKey)).isNotNull().hasSize(2);
  }

  @Test
  public void testDropView() {
    createBranch(DROP_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(DROP_BRANCH, ContentKey.of(dropViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        dropViewKey, viewDefinition, Collections.emptyMap(), getVersion(DROP_BRANCH), s -> s);

    final View icebergView =
        icebergNessieVersionedViewsV0.load(dropViewKey, getVersion(DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViewsV0.drop(dropViewKey, getVersion(DROP_BRANCH));

    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.load(dropViewKey, getVersion(DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    // Drop the view again and expect an exception.
    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.drop(dropViewKey, getVersion(DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testComplexDropView() {
    createBranch(COMPLEX_DROP_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(COMPLEX_DROP_BRANCH, ContentKey.of(complexDropViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        complexDropViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(COMPLEX_DROP_BRANCH),
        s -> s);

    View icebergView =
        icebergNessieVersionedViewsV0.load(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViewsV0.drop(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.load(dropViewKey, getVersion(COMPLEX_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    // Create a new vew with same path and validate it's different to the dropped one.
    icebergNessieVersionedViewsV0.create(
        complexDropViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(COMPLEX_DROP_BRANCH),
        s -> s);

    icebergView =
        icebergNessieVersionedViewsV0.load(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(newViewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViewsV0.drop(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.load(dropViewKey, getVersion(COMPLEX_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testDropViewOnDifferentBranch() {
    createBranch(OLD_DROP_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(OLD_DROP_BRANCH, ContentKey.of(newDropViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        newDropViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(OLD_DROP_BRANCH),
        s -> s);

    createBranch(NEW_DROP_BRANCH, VersionContext.ofBranch(OLD_DROP_BRANCH));

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.replace(
        newDropViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(NEW_DROP_BRANCH),
        s -> s);

    // Drop the view and validate that it's only deleted on the target branch.
    icebergNessieVersionedViewsV0.drop(newDropViewKey, getVersion(OLD_DROP_BRANCH));

    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.load(newDropViewKey, getVersion(OLD_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    final View newIcebergView =
        icebergNessieVersionedViewsV0.load(newDropViewKey, getVersion(NEW_DROP_BRANCH));

    assertThat(newIcebergView).isNotNull();
    assertThat(newIcebergView.currentVersion().versionId()).isEqualTo(2);
    assertThat(newIcebergView.currentVersion().parentId()).isEqualTo(1);
  }

  @Test
  public void testNonGlobalMetadata() {
    createBranch(NON_GLOBAL_METADATA_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(NON_GLOBAL_METADATA_BRANCH, ContentKey.of(nonGlobalMetadataViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.create(
        nonGlobalMetadataViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(NON_GLOBAL_METADATA_BRANCH),
        s -> s);

    createBranch(
        NEW_NON_GLOBAL_METADATA_BRANCH, VersionContext.ofBranch(NON_GLOBAL_METADATA_BRANCH));

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViewsV0.replace(
        nonGlobalMetadataViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(NEW_NON_GLOBAL_METADATA_BRANCH),
        s -> s);

    NessieContent nessieContent =
        fetchContent(nonGlobalMetadataViewKey, NON_GLOBAL_METADATA_BRANCH);
    NessieContent newNessieContent =
        fetchContent(nonGlobalMetadataViewKey, NEW_NON_GLOBAL_METADATA_BRANCH);

    assertThat(nessieContent.getMetadataLocation()).isPresent();
    assertThat(newNessieContent.getMetadataLocation()).isPresent();

    assertThat(nessieContent.getMetadataLocation())
        .isNotEqualTo(newNessieContent.getMetadataLocation());
  }

  @Test
  public void testDummyDialectInNessie() {
    createBranch(DIALECT_BRANCH_V1, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(DIALECT_BRANCH_V1, ContentKey.of(dialectViewKeyV1));

    ViewVersion viewVersion =
        ImmutableViewVersion.builder()
            .timestampMillis(1000)
            .versionId(1)
            .schemaId(0)
            .defaultNamespace(Namespace.of("ns"))
            .addRepresentations(
                ImmutableSQLViewRepresentation.builder()
                    .sql("select * from ns.tbl")
                    .dialect("DremioSQL")
                    .build())
            .build();
    icebergNessieVersionedViewsV1.create(
        dialectViewKeyV1,
        viewVersion,
        Collections.emptyMap(),
        getVersion(DIALECT_BRANCH_V1),
        new Schema(optional(1, "value", Types.IntegerType.get())),
        s -> s);

    NessieContent nessieContent = fetchContent(dialectViewKeyV1, DIALECT_BRANCH_V1);
    assertThat(nessieContent.getViewDialect().get()).isEqualTo(DUMMY_NESSIE_DIALECT);
  }

  @Test
  public void replaceNonExistView() {
    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    assertThatThrownBy(
            () ->
                icebergNessieVersionedViewsV0.replace(
                    nonExistViewKey,
                    viewDefinition,
                    Collections.emptyMap(),
                    getVersion(MAIN_BRANCH),
                    s -> s))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void loadNonExistView() {
    assertThatThrownBy(
            () -> icebergNessieVersionedViewsV0.load(nonExistViewKey, getVersion(MAIN_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void loadNonExistViewDefinition() {
    assertThatThrownBy(
            () ->
                icebergNessieVersionedViewsV0.loadDefinition(
                    nonExistViewKey, getVersion(MAIN_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  private String getViewPath(List<String> viewKey) {
    return Paths.get(temp.toString() + "/" + Joiner.on('/').join(viewKey), "metadata")
        .toAbsolutePath()
        .toString();
  }

  private List<String> getViewMetadatas(List<String> viewKey) {
    return Arrays.stream(new File(getViewPath(viewKey)).listFiles())
        .map(File::getAbsolutePath)
        .filter(f -> f.endsWith(".metadata.json"))
        .collect(Collectors.toList());
  }
}
