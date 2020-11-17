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
package com.dremio.dac.util;

import java.util.ConcurrentModificationException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Resource Utils
 */
public class ResourceUtil {

  public static ConcurrentModificationException correctBadVersionErrorMessage(ConcurrentModificationException e, String objectType, String objectName) {
    final Pattern pattern = Pattern.compile("Cannot delete, expected tag (.*) but found tag (.*)");
    final Matcher matcher = pattern.matcher(e.getMessage());
    if (matcher.find()) {
      return new ConcurrentModificationException(String.format("Cannot delete %s \"%s\", version provided \"%s\" is different from version found \"%s\"",
        objectType, objectName, matcher.group(1), matcher.group(2)));
    } else {
      return e;
    }
  }
}
