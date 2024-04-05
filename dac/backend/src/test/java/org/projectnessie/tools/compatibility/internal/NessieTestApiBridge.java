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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.VersionsToExercise.NESSIE_VERSIONS_PROPERTY;
import static org.projectnessie.tools.compatibility.internal.VersionsToExercise.valueFromResource;
import static org.projectnessie.tools.compatibility.internal.VersionsToExercise.versionsFromValue;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.Version;

/**
 * A bridge into some code of Nessie OSS test frameworks that is not {@code public} now. When the
 * OSS code is enhanced to support Dremio test cases better, this branch class should be removed in
 * favour of interfacing with Nessie OSS code directly.
 */
public class NessieTestApiBridge {

  public static <A extends Annotation> void populateAnnotatedFields(
      ExtensionContext context,
      Object instance,
      Class<A> annotationType,
      Predicate<A> annotationFilter,
      Function<Field, Object> fieldValue) {
    AnnotatedFields.populateAnnotatedFields(
        context, instance, annotationType, annotationFilter, fieldValue);
  }

  public static SortedSet<Version> versionsFromResource() {
    return versionsFromValue(valueFromResource(NESSIE_VERSIONS_PROPERTY));
  }

  /**
   * Starts an in-memory Nessie Server of the specified version and returns its URI. Note: the URI
   * may of may not be suitable for the specified API version class. Check OSS code to confirm.
   * Note: the server is automatically shut down when the test context is destroyed.
   */
  public static URI nessieServer(
      ExtensionContext context, Version version, Class<? extends NessieApi> apiType) {
    ServerKey serverKey =
        ServerKey.forContext(context, version, "In-Memory", Collections.emptyMap());
    OldNessieServer server =
        OldNessieServer.oldNessieServer(context, serverKey, () -> true, c -> {});
    return server.getUri(apiType);
  }
}
