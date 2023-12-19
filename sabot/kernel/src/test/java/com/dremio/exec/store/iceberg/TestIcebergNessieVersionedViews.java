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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.viewdepoc.View;
import org.apache.iceberg.viewdepoc.ViewDefinition;
import org.apache.iceberg.viewdepoc.ViewUtils;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.TableReference;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViews;
import com.dremio.plugins.NessieContent;
import com.google.common.base.Joiner;

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
  private static final String DIALECT_BRANCH = "dialect_branch";

  private static final List<String> createViewKey = Arrays.asList("create", "foo", "bar");
  private static final List<String> replaceViewKey = Arrays.asList("replace", "foo", "bar");
  private static final List<String> dropViewKey = Arrays.asList("drop", "foo", "bar");
  private static final List<String> complexDropViewKey = Arrays.asList("complex_drop", "foo", "bar");
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

    icebergNessieVersionedViews.create(
        createViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(CREATE_BRANCH));

    final View icebergView = icebergNessieVersionedViews.load(createViewKey, getVersion(CREATE_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);
    assertThat(Paths.get(getViewPath(createViewKey))).exists();
    assertThat(getViewMetadatas(createViewKey)).isNotNull().hasSize(1);

    final ViewDefinition icebergViewDefinition =
        icebergNessieVersionedViews.loadDefinition(createViewKey, getVersion(CREATE_BRANCH));
    assertThat(icebergViewDefinition).isEqualTo(viewDefinition);
  }

  @Test
  public void testReplaceView() {
    createBranch(REPLACE_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(REPLACE_BRANCH, ContentKey.of(replaceViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.create(
        replaceViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(REPLACE_BRANCH));

    final View icebergView = icebergNessieVersionedViews.load(replaceViewKey, getVersion(REPLACE_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, NEW_SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.replace(
        replaceViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(REPLACE_BRANCH));

    final View newIcebergView = icebergNessieVersionedViews.load(replaceViewKey, getVersion(REPLACE_BRANCH));

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

    icebergNessieVersionedViews.create(
        dropViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(DROP_BRANCH));

    final View icebergView = icebergNessieVersionedViews.load(dropViewKey, getVersion(DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViews.drop(dropViewKey, getVersion(DROP_BRANCH));

    assertThatThrownBy(() -> icebergNessieVersionedViews.load(dropViewKey, getVersion(DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    // Drop the view again and expect an exception.
    assertThatThrownBy(() -> icebergNessieVersionedViews.drop(dropViewKey, getVersion(DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testComplexDropView() {
    createBranch(COMPLEX_DROP_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(COMPLEX_DROP_BRANCH, ContentKey.of(complexDropViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.create(
        complexDropViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(COMPLEX_DROP_BRANCH));

    View icebergView = icebergNessieVersionedViews.load(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(viewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViews.drop(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThatThrownBy(() -> icebergNessieVersionedViews.load(dropViewKey, getVersion(COMPLEX_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    // Create a new vew with same path and validate it's different to the dropped one.
    icebergNessieVersionedViews.create(
        complexDropViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(COMPLEX_DROP_BRANCH));

    icebergView = icebergNessieVersionedViews.load(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThat(icebergView).isNotNull();
    assertThat(icebergView.currentVersion().versionId()).isEqualTo(1);
    assertThat(icebergView.currentVersion().viewDefinition()).isEqualTo(newViewDefinition);

    // Drop the view and check the existence.
    icebergNessieVersionedViews.drop(complexDropViewKey, getVersion(COMPLEX_DROP_BRANCH));

    assertThatThrownBy(() -> icebergNessieVersionedViews.load(dropViewKey, getVersion(COMPLEX_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testDropViewOnDifferentBranch() {
    createBranch(OLD_DROP_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(OLD_DROP_BRANCH, ContentKey.of(newDropViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.create(
        newDropViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(OLD_DROP_BRANCH));

    createBranch(NEW_DROP_BRANCH, VersionContext.ofBranch(OLD_DROP_BRANCH));

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.replace(
        newDropViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(NEW_DROP_BRANCH));

    // Drop the view and validate that it's only deleted on the target branch.
    icebergNessieVersionedViews.drop(newDropViewKey, getVersion(OLD_DROP_BRANCH));

    assertThatThrownBy(
            () -> icebergNessieVersionedViews.load(newDropViewKey, getVersion(OLD_DROP_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");

    final View newIcebergView =
        icebergNessieVersionedViews.load(newDropViewKey, getVersion(NEW_DROP_BRANCH));

    assertThat(newIcebergView).isNotNull();
    assertThat(newIcebergView.currentVersion().versionId()).isEqualTo(2);
    assertThat(newIcebergView.currentVersion().parentId()).isEqualTo(1);
  }

  @Test
  public void testGlobalMetadata() {
    createBranch(GLOBAL_METADATA_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(GLOBAL_METADATA_BRANCH, Namespace.of(VIEW_IDENTIFIER.namespace().levels()));
    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());
    final TableIdentifier viewIdentifier =
        ViewUtils.toCatalogTableIdentifier(VIEW_IDENTIFIER + "@" + GLOBAL_METADATA_BRANCH);
    nessieExtCatalog.create(viewIdentifier.toString(), viewDefinition, Collections.emptyMap());

    final VersionContext metadataVersionContext = VersionContext.ofBranch(GLOBAL_METADATA_BRANCH);
    createBranch(GLOBAL_NEW_METADATA_BRANCH, metadataVersionContext);

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());
    final TableIdentifier newViewIdentifier =
        ViewUtils.toCatalogTableIdentifier(VIEW_IDENTIFIER + "@" + GLOBAL_NEW_METADATA_BRANCH);
    nessieExtCatalog.replace(newViewIdentifier.toString(), newViewDefinition, Collections.emptyMap());

    final TableReference tr = TableReference.parse(viewIdentifier.name());
    List<String> viewPath = new ArrayList<>(Arrays.asList(viewIdentifier.namespace().levels()));
    viewPath.add(tr.getName());

    NessieContent nessieContent = fetchContent(viewPath, GLOBAL_METADATA_BRANCH);
    NessieContent newNessieContent = fetchContent(viewPath, GLOBAL_NEW_METADATA_BRANCH);

    assertThat(nessieContent.getMetadataLocation()).isPresent();
    assertThat(newNessieContent.getMetadataLocation()).isPresent();

    assertThat(nessieContent.getMetadataLocation()).isNotEqualTo(newNessieContent.getMetadataLocation());

    ViewDefinition firstDef = nessieExtCatalog.getViewCatalog().loadDefinition(viewIdentifier.toString());
    ViewDefinition secondDef = nessieExtCatalog.getViewCatalog().loadDefinition(newViewIdentifier.toString());
    assertThat(firstDef).isNotEqualTo(secondDef);
    assertThat(firstDef.sql()).isEqualTo(SQL);
    assertThat(firstDef.schema().toString()).isEqualTo(SCHEMA.toString());
    assertThat(secondDef.sql()).isEqualTo(NEW_SQL);
    assertThat(secondDef.schema().toString()).isEqualTo(SCHEMA.toString());

    View first = nessieExtCatalog.getViewCatalog().load(viewIdentifier.toString());
    View second = nessieExtCatalog.getViewCatalog().load(newViewIdentifier.toString());
    assertThat(first).isNotEqualTo(second);
    assertThat(first.currentVersion().viewDefinition().schema().toString()).isEqualTo(SCHEMA.toString());
    assertThat(first.currentVersion().viewDefinition().sql()).isEqualTo(SQL);
    assertThat(second.currentVersion().viewDefinition().schema().toString()).isEqualTo(SCHEMA.toString());
    assertThat(second.currentVersion().viewDefinition().sql()).isEqualTo(NEW_SQL);
  }

  @Test
  public void testNonGlobalMetadata() {
    createBranch(NON_GLOBAL_METADATA_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(NON_GLOBAL_METADATA_BRANCH, ContentKey.of(nonGlobalMetadataViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.create(
        nonGlobalMetadataViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(NON_GLOBAL_METADATA_BRANCH));

    createBranch(NEW_NON_GLOBAL_METADATA_BRANCH, VersionContext.ofBranch(NON_GLOBAL_METADATA_BRANCH));

    final ViewDefinition newViewDefinition =
        ViewDefinition.of(NEW_SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.replace(
        nonGlobalMetadataViewKey,
        newViewDefinition,
        Collections.emptyMap(),
        getVersion(NEW_NON_GLOBAL_METADATA_BRANCH));

    NessieContent nessieContent = fetchContent(nonGlobalMetadataViewKey, NON_GLOBAL_METADATA_BRANCH);
    NessieContent newNessieContent = fetchContent(nonGlobalMetadataViewKey, NEW_NON_GLOBAL_METADATA_BRANCH);

    assertThat(nessieContent.getMetadataLocation()).isPresent();
    assertThat(newNessieContent.getMetadataLocation()).isPresent();

    assertThat(nessieContent.getMetadataLocation()).isNotEqualTo(newNessieContent.getMetadataLocation());
  }

  @Test
  public void testViewDialect() {
    createBranch(DIALECT_BRANCH, VersionContext.NOT_SPECIFIED);
    createNamespacesIfMissing(DIALECT_BRANCH, ContentKey.of(createViewKey));

    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    icebergNessieVersionedViews.create(
        createViewKey,
        viewDefinition,
        Collections.emptyMap(),
        getVersion(DIALECT_BRANCH));

    NessieContent nessieContent = fetchContent(createViewKey, DIALECT_BRANCH);
    assertThat(nessieContent.getViewDialect())
      .contains(IcebergNessieVersionedViews.DIALECT);
  }

  @Test
  public void replaceNonExistView() {
    final ViewDefinition viewDefinition =
        ViewDefinition.of(SQL, SCHEMA, CATALOG_NAME, Collections.emptyList());

    assertThatThrownBy(
            () ->
                icebergNessieVersionedViews.replace(
                    nonExistViewKey, viewDefinition, Collections.emptyMap(), getVersion(MAIN_BRANCH)))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void loadNonExistView() {
    assertThatThrownBy(() -> icebergNessieVersionedViews.load(nonExistViewKey, getVersion(MAIN_BRANCH)))
        .isInstanceOf(UserException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void loadNonExistViewDefinition() {
    assertThatThrownBy(
            () -> icebergNessieVersionedViews.loadDefinition(nonExistViewKey, getVersion(MAIN_BRANCH)))
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
