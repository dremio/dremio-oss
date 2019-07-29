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

package com.dremio.exec.planner.logical;

import java.io.IOException;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.store.dfs.FileSystemCreateTableEntry;
import com.dremio.exec.store.dfs.GenericCreateTableEntry;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Interface that provides the info needed to create a new table. A storage engine
 * which supports creating new tables, should implement this interface.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property="type")
@JsonSubTypes({ // TODO: hack until we merge "common" and "java-exec" modules (DRILL-507).
    @Type(name = "filesystem", value = FileSystemCreateTableEntry.class),
    @Type(name = "generic", value = GenericCreateTableEntry.class)
})
public interface CreateTableEntry {

  Writer getWriter(OpProps props, PhysicalOperator child) throws IOException;

  WriterOptions getOptions();

}
