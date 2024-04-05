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
package com.dremio.exec.catalog.dataplane.test;

import static org.junit.platform.commons.support.AnnotationSupport.findAnnotation;
import static org.projectnessie.tools.compatibility.internal.NessieTestApiBridge.populateAnnotatedFields;

import com.dremio.exec.catalog.dataplane.test.DataplaneStorage.StorageType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.engine.ConfigurationParameters;
import org.junit.platform.engine.UniqueId;
import org.projectnessie.junit.engine.MultiEnvTestExtension;

/**
 * Expands tests annotated with @ExtendsWith(MultipleDataplaneStorageExtension.class) to run once
 * against each configured storage backend.
 *
 * <p>Populates fields annotated with {@link PopulateDataplaneStorage} with the configured storage
 * instance.
 */
public class MultipleDataplaneStorageExtension
    implements BeforeAllCallback, BeforeEachCallback, ExecutionCondition, MultiEnvTestExtension {

  public static final String MULTIPLE_DATAPLANE_STORAGE_EXTENSION_SEGMENT_TYPE =
      "multiple-dataplane-storage";

  private static final ExtensionContext.Namespace JUNIT_NAMESPACE =
      ExtensionContext.Namespace.create(MultipleDataplaneStorageExtension.class);

  @Override
  public String segmentType() {
    return MULTIPLE_DATAPLANE_STORAGE_EXTENSION_SEGMENT_TYPE;
  }

  @Override
  public List<String> allEnvironmentIds(ConfigurationParameters configuration) {
    return Arrays.stream(StorageType.values())
        .filter(StorageType::getStorageTypeEnabled)
        .map(StorageType::getFriendlyName)
        .collect(Collectors.toList());
  }

  @Override
  public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
    Optional<SkipForStorageType> skipAnnotation =
        findAnnotation(extensionContext.getTestClass(), SkipForStorageType.class);
    if (skipAnnotation.isPresent()) {
      StorageType storageTypeToSkip = skipAnnotation.get().value();
      if (storageTypeToSkip == storageTypeFromContext(extensionContext)) {
        return ConditionEvaluationResult.disabled(
            String.format("Skipped due to @SkipForStorageType = %s.", storageTypeToSkip));
      }
    }

    return ConditionEvaluationResult.enabled(null);
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    populateFields(extensionContext, null);
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {
    populateFields(extensionContext, extensionContext.getRequiredTestInstance());
  }

  private void populateFields(ExtensionContext extensionContext, Object instance) {
    populateAnnotatedFields(
        extensionContext,
        instance,
        PopulateDataplaneStorage.class,
        a -> true,
        f -> dataplaneStorageForContext(extensionContext));
  }

  private DataplaneStorage dataplaneStorageForContext(ExtensionContext extensionContext) {
    DataplaneStorage dataplaneStorage =
        extensionContext
            .getStore(JUNIT_NAMESPACE)
            .getOrComputeIfAbsent(
                storageTypeFromContext(extensionContext),
                this::dataplaneStorageFactory,
                DataplaneStorage.class);

    dataplaneStorage.start();

    return dataplaneStorage;
  }

  private DataplaneStorage dataplaneStorageFactory(StorageType storageType) {
    switch (storageType) {
      case AWS_S3_MOCK:
        return new S3MockDataplaneStorage();
      case AZURE:
        return new AzureDataplaneStorage();
      default:
        throw new IllegalStateException("Unexpected value: " + storageType);
    }
  }

  private StorageType storageTypeFromContext(ExtensionContext extensionContext) {
    Optional<String> multiEnvId =
        UniqueId.parse(extensionContext.getUniqueId()).getSegments().stream()
            .filter(s -> MULTIPLE_DATAPLANE_STORAGE_EXTENSION_SEGMENT_TYPE.equals(s.getType()))
            .map(UniqueId.Segment::getValue)
            .findFirst();

    return multiEnvId.map(StorageType::fromFriendlyName).orElse(null);
  }
}
