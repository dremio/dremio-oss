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

import org.apache.iceberg.Table;

/**
 * Loads an Iceberg table
 */
public class IcebergTableLoader {
    private Table icebergTable;
    private final IcebergCommand icebergCommand;
    private final String rootPointer;
    public IcebergTableLoader(IcebergCommand icebergCommand) {
        this.icebergCommand = icebergCommand;
        this.rootPointer = icebergCommand.getRootPointer();
    }

    public Table getIcebergTable() {
      if (icebergTable == null) {
        icebergTable = icebergCommand.loadTable();
      }
      return icebergTable;
    }

    public String getRootPointer() {
        return rootPointer;
    }
}
