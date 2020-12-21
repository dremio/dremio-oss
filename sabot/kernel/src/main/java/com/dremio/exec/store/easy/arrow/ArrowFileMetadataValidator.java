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
package com.dremio.exec.store.easy.arrow;

import com.dremio.common.types.MinorType;
import com.dremio.exec.proto.beans.SerializedField;
import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class to validate the contents of an {@link ArrowFileMetadata}
 */
public final class ArrowFileMetadataValidator {

  /**
   * Indicates that the given ArrowFileMetadata has unions that use an outdated IPC format.
   *
   * @param metadata The ArrowFileMetadata to validate.
   */
  public static boolean hasInvalidUnions(ArrowFileMetadata metadata) {
    return !ArrowFileReader.fromBean(metadata).hasArrowMetadataVersion()
      && metadata.getFooter().getFieldList().stream().anyMatch(ArrowFileMetadataValidator::containsUnion);
  }

  @VisibleForTesting
  protected static boolean containsUnion(SerializedField field) {
    if (field == null) {
      return false;
    }

    if (field.getMajorType().getMinorType().equals(MinorType.UNION)) {
      return true;
    }

    return field.getChildList() != null &&
      field.getChildList().stream().anyMatch(ArrowFileMetadataValidator::containsUnion);
  }
}
