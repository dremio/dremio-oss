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
package com.dremio.exec.dotfile;

import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.Path;


public enum DotFileType {
  VIEW
  // ,FORMAT
  // ,STATS
  ;

  private final String ending;

  private DotFileType(){
    this.ending = '.' + name().toLowerCase() + ".meta";
  }

  public boolean matches(FileAttributes attributes){
    return attributes.getPath().getName().endsWith(ending);
  }

  /**
   * For a given parent directory and base file name return complete path including file type specific extensions.
   *
   * @param parentDir Directory where the DotFile is stored.
   * @param name Base file name of the DotFile.
   * @return Path including the extensions that can be used to read/write in filesystem.
   */
  public Path getPath(String parentDir, String name) {
    return Path.of(parentDir).resolve(name + ending);
  }

  /**
   * Return extension string of file type represented by this object.
   *
   * @return File extension.
   */
  public String getEnding() {
    return ending;
  }

  public static final String DOT_FILE_GLOB;

  static{
    StringBuilder b = new StringBuilder();
    b.append(".{");
    for(DotFileType d : values()){
      if(b.length() > 2){
        b.append(',');
      }
      b.append(d.name().toLowerCase());
    }
    b.append("}.meta");
    DOT_FILE_GLOB = b.toString();
  }
}
