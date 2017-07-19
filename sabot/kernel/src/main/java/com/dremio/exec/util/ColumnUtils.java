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
package com.dremio.exec.util;

import java.util.Collection;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.common.expression.SchemaPath;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public final class ColumnUtils {
  public static final SchemaPath STAR_COLUMN = SchemaPath.getSimplePath("*");

  private ColumnUtils() { }

  public static boolean isStarQuery(Collection<SchemaPath> projected) {
    if (projected == null) {
      return false;
    }

    return Iterables.tryFind(projected, new Predicate<SchemaPath>() {
      @Override
      public boolean apply(final SchemaPath path) {
        return Preconditions.checkNotNull(path, "path is required").equals(STAR_COLUMN);
      }
    }).isPresent();
  }

  public static boolean isStarQuery(RelDataType rowType){
    for(RelDataTypeField field : rowType.getFieldList()){
      if(STAR_COLUMN.equals(field.getName())){
        return true;
      }
    }
    return false;
  }

}
