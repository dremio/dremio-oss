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
package com.dremio.exec.planner.sql.handlers.direct;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.catalog.model.dataset.TableVersionType;
import com.dremio.exec.planner.sql.parser.SqlTableVersionSpec;
import java.util.List;

public class CatalogEntityKeyUtil {

  /**
   * Build CatalogEntityKey for a catalog entity from namespace key, version spec from SQL statement
   * and version context of the source. The version context of the entity is decided with the
   * following rules, 1) If the version spec is specified from SQL statement, use it; 2) Or
   * otherwise, use the version context of the source from session context; 3) Set version context
   * to null if it is NOT_SPECIFIED.
   *
   * @param path path of the entity
   * @param statementVersionSpec version spec from SQL statement
   * @param sessionVersion version context of the source of the entity
   * @return CatalogEntityKey of the entity
   */
  public static CatalogEntityKey buildCatalogEntityKey(
      List<String> path, SqlTableVersionSpec statementVersionSpec, VersionContext sessionVersion) {
    TableVersionContext versionContext = null;

    if (statementVersionSpec != null) {
      versionContext = statementVersionSpec.getTableVersionContext();
    }

    if ((versionContext == null || versionContext.getType() == TableVersionType.NOT_SPECIFIED)
        && sessionVersion != null) {
      versionContext = TableVersionContext.of(sessionVersion);
    }

    if (versionContext != null && versionContext.getType() == TableVersionType.NOT_SPECIFIED) {
      versionContext = null;
    }

    return CatalogEntityKey.newBuilder()
        .keyComponents(path)
        .tableVersionContext(versionContext)
        .build();
  }
}
