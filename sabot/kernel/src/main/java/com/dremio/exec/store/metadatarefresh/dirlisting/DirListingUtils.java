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
package com.dremio.exec.store.metadatarefresh.dirlisting;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants;
import com.dremio.io.file.Path;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

/** Utility class related to dir listing. */
public final class DirListingUtils {

  private DirListingUtils() {}

  /**
   * Creates a parent Prel for the dir listing containing the related functions for file name
   * filtering based on the specified regex pattern.
   */
  public static Prel addFileNameRegexFilter(
      RexBuilder rexBuilder, Prel dirListingChildPrel, String fileNameRegex, Path datasetPath) {

    final SqlFunction regexpMatches =
        new SqlFunction(
            "REGEXP_MATCHES",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING);

    final SqlFunction strPos =
        new SqlFunction(
            "STRPOS",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            null,
            OperandTypes.STRING_STRING,
            SqlFunctionCategory.STRING);

    final SqlFunction substring =
        new SqlFunction(
            "SUBSTRING",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.VARCHAR),
            null,
            OperandTypes.STRING_INTEGER,
            SqlFunctionCategory.STRING);

    final SqlFunction add =
        new SqlFunction(
            "ADD",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.explicit(SqlTypeName.INTEGER),
            null,
            OperandTypes.NUMERIC_NUMERIC,
            SqlFunctionCategory.NUMERIC);

    /*
     * The logic here is to first get starting position of dataset path in full file path. then
     * calculate ending position of first occurrence dataset path in full file path. Get substring
     * after ending position which is relative path to storage location. Finally, apply regex
     * match on it.
     */
    String storageLocation = datasetPath.toString() + "/";
    Pair<Integer, RelDataTypeField> fieldWithIndex =
        MoreRelOptUtil.findFieldWithIndex(
            dirListingChildPrel.getRowType().getFieldList(),
            MetadataRefreshExecConstants.DirList.OUTPUT_SCHEMA.FILE_PATH);
    RexNode startAt =
        rexBuilder.makeCall(
            strPos,
            rexBuilder.makeInputRef(fieldWithIndex.right.getType(), fieldWithIndex.left),
            rexBuilder.makeLiteral(storageLocation));
    RexNode endsAt =
        rexBuilder.makeCall(
            add, startAt, rexBuilder.makeLiteral(String.valueOf(storageLocation.length())));
    RexNode remainingPart =
        rexBuilder.makeCall(
            substring,
            rexBuilder.makeInputRef(fieldWithIndex.right.getType(), fieldWithIndex.left),
            endsAt);
    RexNode regexCondition =
        rexBuilder.makeCall(regexpMatches, remainingPart, rexBuilder.makeLiteral(fileNameRegex));

    return FilterPrel.create(
        dirListingChildPrel.getCluster(),
        dirListingChildPrel.getTraitSet(),
        dirListingChildPrel,
        regexCondition);
  }
}
