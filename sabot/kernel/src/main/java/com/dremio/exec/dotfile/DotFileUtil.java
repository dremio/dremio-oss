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

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.util.ArrayList;
import java.util.List;

import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.io.file.PathFilters;

public class DotFileUtil {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DotFileUtil.class);

  private static List<DotFile> filterDotFiles(FileSystem fs, Iterable<FileAttributes> attributes, DotFileType... types){
    List<DotFile> files = new ArrayList<>();
    for(FileAttributes a : attributes){
      DotFile f = DotFile.create(fs, a);
      if(f != null){
        if(types.length == 0){
          files.add(f);
        }else{
          for(DotFileType t : types){
            if(t == f.getType()){
              files.add(f);
            }
          }
        }

      }
    }
    return files;
  }

  public static List<DotFile> getDotFiles(FileSystem fs, Path root, DotFileType... types) throws IOException{
    try (DirectoryStream<FileAttributes> stream = fs.glob(root.resolve("*.meta"), PathFilters.ALL_FILES)) {
      return filterDotFiles(fs, stream, types);
    }
  }

  public static List<DotFile> getDotFiles(FileSystem fs, Path root, String name, DotFileType... types) throws IOException{
    if(!name.endsWith(".meta")) {
      name = name + DotFileType.DOT_FILE_GLOB;
    }

    try (DirectoryStream<FileAttributes> stream = fs.glob(root.resolve(name), PathFilters.ALL_FILES)) {
      return filterDotFiles(fs, stream, types);
    }
  }
}
