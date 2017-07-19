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
package com.dremio.exec.store.dfs;


public enum SchemaMutability {
  ALL(true,true,true,true),
  NONE(false, false, false, false),
  SYSTEM_TABLE(false, true, false, false),
  SYSTEM_TABLE_AND_VIEW(false, true, false, true),
  SYSTEM_VIEW(false, false, false, true),
  USER_TABLE(true, true, false, false),
  USER_VIEW(false, false, true, true),
  USER_TABLE_AND_VIEW(true, true, true, true);

  final boolean anyoneMutateTable;
  final boolean systemMutateTable;
  final boolean anyoneMutateView;
  final boolean systemMutateView;

  private SchemaMutability(boolean anyoneMutateTable, boolean systemMutateTable, boolean anyoneMutateView,
      boolean systemMutateView) {
    this.anyoneMutateTable = anyoneMutateTable;
    this.systemMutateTable = systemMutateTable;
    this.anyoneMutateView = anyoneMutateView;
    this.systemMutateView = systemMutateView;
  }

  public boolean hasMutationCapability(MutationType type, boolean isSystemUser){
    if(isSystemUser){
      switch(type){
      case VIEW:
        return systemMutateView;
      case TABLE:
        return systemMutateTable;
      default:
        throw new IllegalStateException();
      }
    }


    switch(type){
    case VIEW:
      return anyoneMutateView;
    case TABLE:
      return anyoneMutateTable;
    default:
      throw new IllegalStateException();
    }

  }

  public static enum MutationType {
    VIEW, TABLE
  }
}