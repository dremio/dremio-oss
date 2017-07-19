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
package com.dremio.exec.dotfile;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileStatus;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.exec.store.dfs.FileSystemWrapper;
import com.google.common.base.Preconditions;

public class DotFile {

  private FileStatus status;
  private DotFileType type;
  private FileSystemWrapper fs;

  public static DotFile create(FileSystemWrapper fs, FileStatus status){
    for(DotFileType d : DotFileType.values()){
      if(!status.isDirectory() && d.matches(status)){
        return new DotFile(fs, status, d);
      }
    }
    return null;
  }

  private DotFile(FileSystemWrapper fs, FileStatus status, DotFileType type){
    this.fs = fs;
    this.status = status;
    this.type = type;
  }

  public DotFileType getType(){
    return type;
  }

  /**
   * @return Return owner of the file in underlying file system.
   */
  public String getOwner() {
    return status.getOwner();
  }

  /**
   * Return base file name without the parent directory and extensions.
   * @return Base file name.
   */
  public String getBaseName() {
    final String fileName = status.getPath().getName();
    return fileName.substring(0, fileName.lastIndexOf(type.getEnding()));
  }

  public View getView(LogicalPlanPersistence lpPersistence) throws IOException {
    Preconditions.checkArgument(type == DotFileType.VIEW);
    try(InputStream is = fs.open(status.getPath())){
      return lpPersistence.getMapper().readValue(is, View.class);
    }
  }
}
