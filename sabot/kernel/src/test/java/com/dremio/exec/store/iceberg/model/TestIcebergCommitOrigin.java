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
package com.dremio.exec.store.iceberg.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;

import com.dremio.exec.catalog.VersionedPlugin.EntityType;

public class TestIcebergCommitOrigin {

  private static final ContentKey TABLE_KEY = ContentKey.of("foo", "bar", "tablexyz");
  private static final ContentKey VIEW_KEY = ContentKey.of("foo", "bar", "viewxyz");

  private void assertTable(IcebergCommitOrigin commitOrigin, String expectedCommitMsg) {
    assertThat(commitOrigin.createCommitMessage(TABLE_KEY.toString(), EntityType.ICEBERG_TABLE))
      .isEqualTo(expectedCommitMsg);
  }

  @Test
  public void testForTable() {
    assertTable(IcebergCommitOrigin.CREATE_TABLE,
      "CREATE TABLE foo.bar.tablexyz");
    assertTable(IcebergCommitOrigin.DROP_TABLE,
      "DROP TABLE foo.bar.tablexyz");
    assertTable(IcebergCommitOrigin.OPTIMIZE_REWRITE_DATA_TABLE,
      "OPTIMIZE REWRITE DATA on TABLE foo.bar.tablexyz");
    assertTable(IcebergCommitOrigin.TRUNCATE_TABLE,
      "TRUNCATE on TABLE foo.bar.tablexyz");
    assertTable(IcebergCommitOrigin.ROLLBACK_TABLE,
      "ROLLBACK on TABLE foo.bar.tablexyz");
    assertTable(IcebergCommitOrigin.FULL_METADATA_REFRESH,
      "FULL METADATA REFRESH on TABLE foo.bar.tablexyz");
  }

  private void assertView(IcebergCommitOrigin commitOrigin, String expectedCommitMsg) {
    assertThat(commitOrigin.createCommitMessage(VIEW_KEY.toString(), EntityType.ICEBERG_VIEW))
      .isEqualTo(expectedCommitMsg);
  }

  @Test
  public void testForView() {
    assertView(IcebergCommitOrigin.CREATE_VIEW,
      "CREATE VIEW foo.bar.viewxyz");
    assertView(IcebergCommitOrigin.DROP_VIEW,
      "DROP VIEW foo.bar.viewxyz");
    assertView(IcebergCommitOrigin.ALTER_VIEW,
      "ALTER on VIEW foo.bar.viewxyz");
  }

  @Test
  public void testReadOnly() {
    assertThatThrownBy(() ->
      IcebergCommitOrigin.READ_ONLY.createCommitMessage(TABLE_KEY.toString(),
        EntityType.ICEBERG_TABLE))
      .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testForTableWithView() {
    assertThatThrownBy(() ->
      IcebergCommitOrigin.CREATE_TABLE.createCommitMessage(VIEW_KEY.toString(),
        EntityType.ICEBERG_VIEW))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Mismatched entity type");
  }

  @Test
  public void testForViewWithTable() {
    assertThatThrownBy(() ->
      IcebergCommitOrigin.CREATE_VIEW.createCommitMessage(TABLE_KEY.toString(),
        EntityType.ICEBERG_TABLE))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Mismatched entity type");
  }

  @Test
  public void testForFolder() {
    assertThatThrownBy(() ->
      IcebergCommitOrigin.CREATE_VIEW.createCommitMessage(TABLE_KEY.toString(),
        EntityType.FOLDER))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Unsupported entity type");
  }
}
