/*
 * Copyright (C) 2017 Dremio Corporation
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

import static com.dremio.exec.planner.sql.handlers.direct.SimpleCommandResult.successful;
import static java.util.Collections.singletonList;

import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.curator.shaded.com.google.common.collect.Iterables;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlForgetTable;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.ImmutableList;

/**
 * Handler for <code>FORGET TABLE tblname</code> command.
 */
public class ForgetTableHandler extends SimpleDirectHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ForgetTableHandler.class);

  private final SchemaPlus defaultSchema;
  private final NamespaceService namespaceService;
  private final AccelerationManager accel;

  public ForgetTableHandler(SchemaPlus defaultSchema, NamespaceService namespaceService, AccelerationManager accel) {
    this.defaultSchema = defaultSchema;
    this.namespaceService = namespaceService;
    this.accel = accel;
  }

  @Override
  public List<SimpleCommandResult> toResult(String sql, SqlNode sqlNode) throws Exception {
    final SqlForgetTable sqlForgetTable = SqlNodeUtil.unwrap(sqlNode, SqlForgetTable.class);

    boolean verified;
    List<String> path;
    try {
      path = SchemaUtilities.verify(defaultSchema, sqlForgetTable.getTable()).getPath();
      verified = true;
    } catch(Exception ex){
      logger.warn("Failure to load verify table before forgetting. Attempting to do case sensitive direct load as fallback.");
      path = verifySchemaAndComposePath(defaultSchema, sqlForgetTable.getTable());
      verified = false;
      if(path == null){
        throw UserException.parseError().message("Unable to find table.").build(logger);
      }
    }

    final NamespaceKey tableNSKey = new NamespaceKey(path);

    final DatasetConfig datasetConfig;
    try {
      datasetConfig = namespaceService.getDataset(tableNSKey);
    } catch (NamespaceNotFoundException ex) {
      if(verified){
        throw UserException.parseError().message("Unable to find table.").build(logger);
      } else {
        throw UserException.parseError().message("Unable to retrieve table due to issues with metadata. Please make sure you use the exact case the table was constructed with when dropping metadata.").build(logger);
      }
    }

    if (!RefreshTableHandler.ALTER_METADATA_TYPES.contains(datasetConfig.getType()))  {
      throw UserException.parseError().message("ALTER TABLE <TABLE> FORGET METADATA is only supported on physical datasets").build(logger);
    }

    // TODO: There could concurrency issue which could cause the version to be invalid.
    namespaceService.deleteDataset(tableNSKey, datasetConfig.getVersion());

    accel.dropAcceleration(path, false);

    return singletonList(successful(String.format("Successfully removed table '%s' from namespace (and associated reflections).", sqlForgetTable.getTable().toString())));
  }


  private static List<String> verifySchemaAndComposePath(final SchemaPlus defaultSchema, SqlIdentifier identifier){
    if(identifier.isSimple()){
      return ImmutableList.of(identifier.getSimple());
    } else {
      SchemaPlus plus = SchemaUtilities.findSchema(defaultSchema, identifier.names.subList(0, identifier.names.size() - 1));
      if(plus == null){
        return null;
      }

      List<String> schema = SchemaUtilities.getSchemaPathAsList(plus);
      return ImmutableList.copyOf(Iterables.concat(schema, ImmutableList.of(identifier.names.get(identifier.names.size() - 1))));
    }
  }

}
