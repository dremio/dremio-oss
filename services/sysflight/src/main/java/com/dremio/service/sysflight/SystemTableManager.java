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

package com.dremio.service.sysflight;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.FlightProtos.SysFlightTicket;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.FlightProducer.StreamListener;
import org.apache.arrow.vector.types.pojo.Schema;

/** System table manager interface. */
public interface SystemTableManager extends AutoCloseable {

  void streamData(SysFlightTicket ticket, ServerStreamListener listener) throws Exception;

  void listSchemas(StreamListener<FlightInfo> listener);

  Schema getSchema(String datasetName);

  void setRecordBatchSize(int recordBatchSize);

  /** Enum to check for the supported tables. */
  public enum TABLES implements SystemTables {
    JOBS("jobs"),
    MATERIALIZATIONS("materializations"),
    REFLECTIONS("reflections"),
    REFLECTION_DEPENDENCIES("reflection_dependencies"),
    JOBS_RECENT("jobs_recent");

    final String name;

    private TABLES(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return this.name;
    }

    public static TABLES fromString(String input) {
      for (TABLES t : TABLES.values()) {
        if (t.name.equalsIgnoreCase(input)) {
          return t;
        }
      }
      throwUnsupportedException(String.format("'%s' system table is not supported.", input));
      return null;
    }
  }

  /** Enum to check for the supported table functions. */
  public enum TABLE_FUNCTIONS {
    REFLECTION_LINEAGE("reflection_lineage");

    final String name;

    private TABLE_FUNCTIONS(String name) {
      this.name = name;
    }

    public String getName() {
      return this.name;
    }

    public static TABLE_FUNCTIONS fromString(String input) {
      for (TABLE_FUNCTIONS t : TABLE_FUNCTIONS.values()) {
        if (t.name.equalsIgnoreCase(input)) {
          return t;
        }
      }
      throwUnsupportedException(
          String.format("'%s' system table function is not supported.", input));
      return null;
    }
  }

  static void throwUnsupportedException(String msg) {
    throw UserException.unsupportedError().message(msg).buildSilently();
  }
}
