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
package com.dremio.exec.planner.sql.handlers.query;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.ValidationException;

import com.dremio.exec.planner.StatelessRelShuttleImpl;

public final class ScanLimitValidator {

  static void ensureLimit(RelNode relNode, long limit) throws ValidationException {
    Validator validator = new Validator(limit);
    try {
      relNode.accept(validator);
    } catch (ScanLimitException ex) {
      throw new ValidationException(
          String.format(
              "At most %s columns including leaf level fields of complex type are allowed to be scanned, but the query is scanning %s. " +
                  "Please include the columns you want to be returned from the query and try again",
              limit,
              ex.actualFieldCount
          )
      );
    }
  }


  private static final class ScanLimitException extends RuntimeException {
    private final int actualFieldCount;

    public ScanLimitException(int actualFieldCount) {
      this.actualFieldCount = actualFieldCount;
    }
  }

  private static final class Validator extends StatelessRelShuttleImpl {
    private final long limit;

    public Validator(long limit) {
      this.limit = limit;
    }

    @Override
    public RelNode visit(TableScan scan) {
      int totalNumberOfFields = getTotalFieldCount(scan.getRowType());
      if (totalNumberOfFields > limit) {
        throw new ScanLimitException(totalNumberOfFields);
      }
      return super.visit(scan);
    }
  }

  private static int getTotalFieldCount(RelDataType dataType) {
    if (!dataType.isStruct()) {
      return 1;
    }
    return dataType.getFieldList()
        .stream()
        .reduce(0, (acc, el) -> acc + getTotalFieldCount(el.getType()), Integer::sum);
  }
}
