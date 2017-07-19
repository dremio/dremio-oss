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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.sql.SchemaUtilities;
import com.dremio.exec.planner.sql.parser.SqlShowFiles;
import com.dremio.exec.store.SimpleSchema;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.dremio.exec.work.foreman.ForemanSetupException;

public class ShowFileHandler implements SqlDirectHandler<ShowFilesCommandResult> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  private final SchemaPlus defaultSchema;

  public ShowFileHandler(SchemaPlus defaultSchema) {
    super();
    this.defaultSchema = defaultSchema;
  }

  @Override
  public List<ShowFilesCommandResult> toResult(String sql, SqlNode sqlNode) throws ValidationException, RelConversionException,
  IOException, ForemanSetupException {

    SqlIdentifier from = ((SqlShowFiles) sqlNode).getDb();
    List<ShowFilesCommandResult> rows = new ArrayList<>();

    FileSystemWrapper fs = null;
    String defaultLocation = null;
    String fromDir = "./";

    SchemaPlus schemaPlus = defaultSchema;

    // Show files can be used without from clause, in which case we display the files in the default schema
    if (from != null) {
      // We are not sure if the full from clause is just the schema or includes table name,
      // first try to see if the full path specified is a schema
      schemaPlus = SchemaUtilities.findSchema(defaultSchema, from.names);
      if (schemaPlus == null) {
        // Entire from clause is not a schema, try to obtain the schema without the last part of the specified clause.
        schemaPlus = SchemaUtilities.findSchema(defaultSchema, from.names.subList(0, from.names.size() - 1));
        fromDir = fromDir + from.names.get((from.names.size() - 1));
      }

      if (schemaPlus == null) {
        throw UserException.validationError()
            .message("Invalid FROM/IN clause [%s]", from.toString())
            .build(logger);
      }
    }

    SimpleSchema schema;
    try {
      schema = schemaPlus.unwrap(SimpleSchema.class);
    } catch (ClassCastException e) {
      throw UserException.validationError()
          .message("SHOW FILES is supported in workspace type schema only. Schema [%s] is not a workspace schema.",
              SchemaUtilities.getSchemaPath(schemaPlus))
          .build(logger);
    }

    // Get the file system object
    fs = schema.getFileSystem();

    // Get the default path
    defaultLocation = schema.getDefaultLocation();

    for (FileStatus fileStatus : fs.list(false, new Path(defaultLocation, fromDir))) {
      ShowFilesCommandResult result = new ShowFilesCommandResult(fileStatus.getPath().getName(), fileStatus.isDir(),
          !fileStatus.isDirectory(), fileStatus.getLen(),
          fileStatus.getOwner(), fileStatus.getGroup(),
          fileStatus.getPermission().toString(),
          fileStatus.getAccessTime(), fileStatus.getModificationTime());
      rows.add(result);
    }

    return rows;
  }

  @Override
  public Class<ShowFilesCommandResult> getResultType() {
    return ShowFilesCommandResult.class;
  }

}
