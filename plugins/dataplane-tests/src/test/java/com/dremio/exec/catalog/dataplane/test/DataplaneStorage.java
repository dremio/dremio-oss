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

import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A simple abstraction for interacting with the underlying storage of a Dataplane Source during
 * tests.
 */
public interface DataplaneStorage extends ExtensionContext.Store.CloseableResource {

  void start();

  enum StorageType {
    AWS_S3_MOCK("s3mock", () -> isStorageTypeEnabled("aws")),
    AZURE(
        "azure",
        () ->
            isStorageTypeEnabled("azure")
                &&
                // Only use this type if we have credentials
                System.getenv("AZURE_STORAGE_DATAPLANE_ACCOUNT_NAME") != null
                && System.getenv("AZURE_STORAGE_DATAPLANE_ACCOUNT_KEY") != null),
    GCS_MOCK("gcsMock", () -> isStorageTypeEnabled("gcs"));

    private static final Set<String> ENABLED_STORAGE_TYPES =
        Set.of(
            System.getProperty("tests.dataplane.enabled.storage_types", "aws,azure,gcs")
                .toLowerCase(Locale.ROOT)
                .split(","));

    private static boolean isStorageTypeEnabled(String storageType) {
      return ENABLED_STORAGE_TYPES.contains(storageType.toLowerCase(Locale.ROOT));
    }

    private static final Map<String, StorageType> friendlyNameToStorageTypeMap = new HashMap<>();

    static {
      Arrays.stream(StorageType.values())
          .forEach(v -> friendlyNameToStorageTypeMap.put(v.getFriendlyName(), v));
    }

    private final String friendlyName;
    private final BooleanSupplier storageTypeEnabled;

    StorageType(String friendlyName, BooleanSupplier storageTypeEnabled) {
      this.friendlyName = friendlyName;
      this.storageTypeEnabled = storageTypeEnabled;
    }

    public String getFriendlyName() {
      return friendlyName;
    }

    public static StorageType fromFriendlyName(String friendlyName) {
      return friendlyNameToStorageTypeMap.get(friendlyName);
    }

    public boolean getStorageTypeEnabled() {
      return storageTypeEnabled.getAsBoolean();
    }
  }

  StorageType getType();

  default int getPort() {
    throw new UnsupportedOperationException();
  }

  enum BucketSelection {
    PRIMARY_BUCKET,
    ALTERNATE_BUCKET
  }

  String getBucketName(BucketSelection bucketSelection);

  boolean doesObjectExist(BucketSelection bucketSelection, String objectPath);

  void putObject(String objectPath, File file);

  void deleteObject(BucketSelection bucketSelection, String objectPath);

  void deleteObjects(BucketSelection bucketSelection, List<String> objectPaths);

  default Stream<String> listObjectNames(BucketSelection bucketSelection, String filterPath) {
    return listObjectNames(bucketSelection, filterPath, objectNameFilter -> true);
  }

  Stream<String> listObjectNames(
      BucketSelection bucketSelection, String filterPath, Predicate<String> objectNameFilter);

  NessiePluginConfig preparePluginConfig(BucketSelection bucketSelection, String nessieEndpoint);

  FileIO getFileIO();

  String getWarehousePath();
}
