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
package com.dremio.exec.planner.acceleration;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.DremioSqlToRelConverter;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.service.users.SystemUser;

public class ExternalMaterializationDescriptor extends MaterializationDescriptor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalMaterializationDescriptor.class);

  private final List<String> virtualDatasetPath;
  public ExternalMaterializationDescriptor(ReflectionInfo reflection,
                                           String materializationId,
                                           String version,
                                           List<String> virtualDatasetPath,
                                           List<String> physicalDatasetPath) {
    super(reflection, materializationId, version, Long.MAX_VALUE, null, physicalDatasetPath, 0D, 0,
        Collections.emptyList(), IncrementalUpdateSettings.NON_INCREMENTAL, null, Long.MIN_VALUE, StrippingFactory.NO_STRIP_VERSION);
    this.virtualDatasetPath = virtualDatasetPath;
  }

  @Override
  public DremioMaterialization getMaterializationFor(SqlConverter converter) {
    String queryPath = PathUtils.constructFullPath(virtualDatasetPath);
    String targetPath = PathUtils.constructFullPath(getPath());

    final RelNode queryRel = DremioSqlToRelConverter.expandView(null, SystemUser.SYSTEM_USERNAME,
        String.format("select * from %s", queryPath), null, converter, null).rel;
    RelNode tableRel = DremioSqlToRelConverter.expandView(null, SystemUser.SYSTEM_USERNAME,
        String.format("select * from %s", targetPath), null, converter, null).rel;

    if (!MoreRelOptUtil.areRowTypesEqual(queryRel.getRowType(), tableRel.getRowType(), true, false)) {
      throw UserException.validationError()
        .message("External reflection schema does not match Dataset schema")
        .addContext("Dataset schema", queryRel.getRowType().toString())
        .addContext("Reflection schema", tableRel.getRowType().toString())
        .build(logger);
    }
    if (!MoreRelOptUtil.areRowTypesEqual(queryRel.getRowType(), tableRel.getRowType(), true, true)) {
      tableRel = MoreRelOptUtil.createCastRel(tableRel, queryRel.getRowType());
    }
    return new DremioMaterialization(
      tableRel,
      queryRel,
      IncrementalUpdateSettings.NON_INCREMENTAL,
      null,
      reflection,
      getMaterializationId(),
      null,
      Long.MAX_VALUE,
      getStrippedPlanHash() == null,
      StrippingFactory.LATEST_STRIP_VERSION,
      null
    );
  }

}
