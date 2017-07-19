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

import java.sql.Timestamp;

import org.joda.time.LocalDateTime;

public class ShowFilesCommandResult {

  /* Fields that will be returned as columns
   * for a 'SHOW FILES' command
   */

  // Name of the file
  public String name;

  // Is it a directory
  public boolean isDirectory;

  // Is it a file
  public boolean isFile;

  // Length of the file
  public long length;

  // File owner
  public String owner;

  // File group
  public String group;

  // File permissions
  public String permissions;

  // Access Time
  public Timestamp accessTime;

  // Modification Time
  public Timestamp modificationTime;

  public ShowFilesCommandResult(String name,
                                boolean isDirectory,
                                boolean isFile,
                                long length,
                                String owner,
                                String group,
                                String permissions,
                                long accessTime,
                                long modificationTime) {
    this.name = name;
    this.isDirectory = isDirectory;
    this.isFile = isFile;
    this.length = length;
    this.owner = owner;
    this.group = group;
    this.permissions = permissions;

    // Get the timestamp in UTC because Dremio's internal TIMESTAMP stores time in UTC
    LocalDateTime at = new LocalDateTime(accessTime);
    this.accessTime = new Timestamp(com.dremio.common.util.DateTimes.toMillis(at));

    LocalDateTime mt = new LocalDateTime(modificationTime);
    this.modificationTime = new Timestamp(com.dremio.common.util.DateTimes.toMillis(mt));
  }
}
