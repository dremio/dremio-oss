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

import java.util.List;

import org.apache.arrow.vector.util.DateUtility;

import com.dremio.exec.proto.CoordExecRPC.FragmentPriority;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.google.common.collect.Lists;

public class Utilities {
    /*
     * From the context, get the query id, major fragment id, minor fragment id. This will be used as the file name to
     * which we will dump the incoming buffer data
     */
  public static String getFileNameForQueryFragment(FragmentHandle handle, String location, String tag) {

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName = String.format("%s//%s_%s_%s_%s", location, qid, majorFragmentId, minorFragmentId, tag);

    return fileName;
  }

  /**
   * Compares two lists' (unordered) content without needing elements to be Comparable.
   * @param A list to compare
   * @param B list to compare
   * @return true iff A and B have the same elements
   */
  public static boolean listsUnorderedEquals(final List<?> A, final List<?> B) {
    if (A == null && B == null) {
      return true;
    }
    if (A != null && B != null && A.size() == B.size()) {
      List<?> copyOfA = Lists.newLinkedList(A);
      for (Object elemInB : B) {
        if (!copyOfA.remove(elemInB)) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  public static QueryContextInformation createQueryContextInfo(final String defaultSchemaName) {
    return createQueryContextInfo(defaultSchemaName, null, Long.MAX_VALUE);
  }
  /**
   * Create QueryContextInformation with given <i>defaultSchemaName</i>. Rest of the members of the
   * QueryContextInformation is derived from the current state of the process.
   *
   * @param defaultSchemaName
   * @return
   */
  public static QueryContextInformation createQueryContextInfo(final String defaultSchemaName, QueryPriority priority, long maxAllocation) {
    final long queryStartTime = System.currentTimeMillis();
    final int timeZone = DateUtility.getIndex(System.getProperty("user.timezone"));
    FragmentPriority.Builder priorityBuilder = FragmentPriority.newBuilder();
    if(priority != null){
      priorityBuilder.setWorkloadClass(priority.getWorkloadClass());
    } else {
      priorityBuilder.setWorkloadClass(WorkloadClass.GENERAL);
    }
    return QueryContextInformation.newBuilder()
        .setDefaultSchemaName(defaultSchemaName)
        .setQueryStartTime(queryStartTime)
        .setTimeZone(timeZone)
        .setPriority(priorityBuilder)
        .setQueryMaxAllocation(maxAllocation)
        .build();
  }

}
