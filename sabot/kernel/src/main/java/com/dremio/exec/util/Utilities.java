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
package com.dremio.exec.util;

import com.dremio.common.util.JodaDateUtility;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordExecRPC.FragmentPriority;
import com.dremio.exec.proto.CoordExecRPC.QueryContextInformation;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.exec.proto.UserBitShared.WorkloadType;
import com.dremio.exec.proto.UserProtos.QueryPriority;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Locale;

public class Utilities {
  /*
   * From the context, get the query id, major fragment id, minor fragment id. This will be used as the file name to
   * which we will dump the incoming buffer data
   */
  public static String getFileNameForQueryFragment(
      FragmentHandle handle, String location, String tag) {

    String qid = QueryIdHelper.getQueryId(handle.getQueryId());

    int majorFragmentId = handle.getMajorFragmentId();
    int minorFragmentId = handle.getMinorFragmentId();

    String fileName =
        String.format("%s//%s_%s_%s_%s", location, qid, majorFragmentId, minorFragmentId, tag);

    return fileName;
  }

  /**
   * Compares two lists' (unordered) content without needing elements to be Comparable.
   *
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
    return createQueryContextInfo(defaultSchemaName, null, Long.MAX_VALUE, null);
  }

  /**
   * Create QueryContextInformation with given <i>defaultSchemaName</i>. Rest of the members of the
   * QueryContextInformation is derived from the current state of the process.
   *
   * @param defaultSchemaName
   * @return
   */
  public static QueryContextInformation createQueryContextInfo(
      final String defaultSchemaName,
      QueryPriority priority,
      long maxAllocation,
      QueryId lastQueryId) {
    final long queryStartTime = System.currentTimeMillis();
    final int timeZone = JodaDateUtility.getIndex(System.getProperty("user.timezone"));
    FragmentPriority.Builder priorityBuilder = FragmentPriority.newBuilder();
    if (priority != null) {
      priorityBuilder.setWorkloadClass(priority.getWorkloadClass());
    } else {
      priorityBuilder.setWorkloadClass(WorkloadClass.GENERAL);
    }

    QueryContextInformation.Builder builder =
        QueryContextInformation.newBuilder()
            .setDefaultSchemaName(defaultSchemaName)
            .setQueryStartTime(queryStartTime)
            .setTimeZone(timeZone)
            .setPriority(priorityBuilder)
            .setQueryMaxAllocation(maxAllocation);

    return (lastQueryId == null ? builder : builder.setLastQueryId(lastQueryId)).build();
  }

  public static WorkloadType getWorkloadType(
      QueryPriority queryPriority, UserBitShared.RpcEndpointInfos clientInfos) {
    if (queryPriority == null
        || queryPriority.getWorkloadType() == null
        || WorkloadType.UNKNOWN.equals(queryPriority.getWorkloadType())) {
      return getByClientType(clientInfos);
    }
    return queryPriority.getWorkloadType();
  }

  public static UserBitShared.WorkloadType getByClientType(
      UserBitShared.RpcEndpointInfos clientInfos) {
    if (clientInfos == null) {
      return WorkloadType.UNKNOWN;
    }

    final String name = clientInfos.getName().toLowerCase(Locale.ROOT);
    if (name.contains("dremio-to-dremio")) {
      return WorkloadType.D2D;
    }

    if (name.contains("jdbc") || name.contains("java")) {
      return WorkloadType.JDBC;
    }

    if (name.contains("odbc") || name.contains("c++")) {
      return WorkloadType.ODBC;
    }

    if (name.contains("flight")) {
      return WorkloadType.FLIGHT;
    }

    return WorkloadType.UNKNOWN;
  }

  public static String getHumanReadableWorkloadType(WorkloadType workloadType) {
    switch (workloadType) {
      case DDL:
        return "DDL"; // not yet configurable via UI
      case INTERNAL_RUN:
        return "Internal Run";
      case INTERNAL_PREVIEW:
        return "Internal Preview";
      case D2D:
        return "D2D";
      case JDBC:
        return "JDBC";
      case ODBC:
        return "ODBC";
      case ACCELERATOR:
        return "Reflections";
      case REST:
        return "REST";
      case UI_PREVIEW:
        return "UI Preview";
      case UI_RUN:
        return "UI Run";
      case UI_DOWNLOAD:
        return "UI Download";
      case FLIGHT:
        return "Flight";
      case METADATA_REFRESH:
        return "Metadata Refresh";
      case UNKNOWN:
      default:
        return "Other";
    }
  }

  public static boolean isAccelerationType(final WorkloadType queryType) {
    return isAccelerationType(getHumanReadableWorkloadType(queryType));
  }

  public static boolean isAccelerationType(final String queryType) {
    return queryType != null
        && queryType.equals(getHumanReadableWorkloadType(WorkloadType.ACCELERATOR));
  }
}
