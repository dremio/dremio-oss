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
package com.dremio.provision.yarn;

import static com.dremio.common.TestProfileHelper.assumeMaprProfile;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

import com.dremio.provision.yarn.service.YarnDefaultsConfigurator;
import com.google.common.base.StandardSystemProperty;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.Test;

/** Test cases for {@code com.dremio.provision.yarn.service.YarnDefaultsConfigurator} */
public class TestYarnDefaultsConfigurator {

  @Test
  public void testMaprAppClasspath() {
    assumeMaprProfile();

    final Set<String> jars =
        Arrays.stream(StandardSystemProperty.JAVA_CLASS_PATH.value().split(":"))
            .map(Paths::get)
            .map(Path::getFileName)
            .map(Path::toString)
            .collect(Collectors.toSet());

    // Sanity check to make sure that jars required to start app are present in the test classpath
    // test itself does not require those jars but it should prevent naming mismatches
    final String appClassPath = YarnDefaultsConfigurator.MapRYarnDefaults.getAppClassPath();
    for (final String path : appClassPath.split(",")) {
      // Just checking filename is present since the layout depends on the actual distribution
      // config
      final String filename = Paths.get(path).getFileName().toString();

      assertTrue(
          format("jar %s not present in classpath (%s)", filename, jars),
          checkExist(jars, filename));
    }
  }

  private static boolean checkExist(Set<String> jars, String filename) {
    final Pattern pattern = Pattern.compile(filename);

    for (final String jar : jars) {
      if (pattern.matcher(jar).matches()) {
        return true;
      }
    }

    return false;
  }
}
