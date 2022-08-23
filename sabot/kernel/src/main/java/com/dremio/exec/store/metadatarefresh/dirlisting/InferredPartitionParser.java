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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.io.file.Path;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Class responsible for inferring partition columns by parsing file paths. Partition column names are derived from the
 * names of the directories, instead of implicit names like dir0, dir1 and so on.
*/
public class InferredPartitionParser extends PartitionParser {
  private List<String> currentNames =  new ArrayList<>(), currentValues = new ArrayList<>();

  private List<String> partitionNames;

  public static List<String> acceptedNullPartition = ImmutableList.of("__HIVE_DEFAULT_PARTITION__", "null");

  private boolean partitionNamesInitialized;
  private Path workingPath;

  protected InferredPartitionParser(Path rootPath) {
    super(rootPath);
    this.partitionNamesInitialized = false;
  }

  @Override
  public IcebergPartitionData parsePartitionToPath(Path path) {
    workingPath = path;
    String[] dirs = Path.withoutSchemeAndAuthority(this.rootPath).relativize(Path.withoutSchemeAndAuthority(path)).toString().split(Path.SEPARATOR);

    validateFormatForAllDirs(dirs);
    parsePath(dirs);
    IcebergPartitionData icebergPartitionData = buildIcebergPartitionData(dirs, currentNames, currentValues);

    currentValues.clear();
    currentNames.clear();
    return icebergPartitionData;
  }

  private void parsePath(String[] dirs) {
    for(int i = 0; i < dirs.length - 1; i++) {
      String[] partitions = dirs[i].split("=");

      String name = null;
      String value = null;

      if(partitions.length < 1 || partitions.length > 2) {
        throw UserException.parseError()
          .message(String.format("Invalid partition structure was specified."))
          .buildSilently();
      }

      if(partitions.length == 1) {
        name = partitions[0];
        value = null;
      } else if (partitions.length == 2) {
        name = partitions[0];
        value = partitions[1];

        if(acceptedNullPartition.contains(value)) {
          value = null;
        }
      }

      currentNames.add(name);
      currentValues.add(value);
    }
    validatePartitionStructure();
  }

  // Validates that partition structure remains same throughout.
  // All the missing subdirectories should be created explicitly by user.
  void validatePartitionStructure() {
    if(partitionNamesInitialized) {
      Preconditions.checkState(partitionNames.equals(currentNames), String.format("Invalid partition structure was specified. " +
        "Earlier inferred partition names were %s. Parsing current path" +
        " resulted in partition names %s. Path %s. Please correct directory structure if possible.", partitionNames, currentNames, workingPath));
    } else {
      partitionNames = new ArrayList<>(currentNames);
      partitionNamesInitialized = true;
    }
  }

  // Validates that all directory names are in required format, i.e., name=value.
  // Directory structures like '/id=1/data=2/tempDir/parquetFile' are not allowed.
  void validateFormatForAllDirs(String[] dirs) {
    for(int i = 0; i < dirs.length - 1; i++) {
      Preconditions.checkState(StringUtils.countMatches(dirs[i], "=") == 1, String.format("All the directories should have = in the partition structure. Path %s", workingPath));
    }
  }
}
