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
package com.dremio.plugins.dataplane.store;

import java.util.regex.Pattern;

@SuppressWarnings("JavadocLinkAsPlainText") // To keep javadocs simple in editor
public final class CloudStoragePathValidator {

  private static final String AWS_S3_BUCKET_NAME_REGEX =
      "[a-z0-9](?!.*\\.\\.)[a-z0-9.\\-]{1,61}[a-z0-9]";
  // Warning: only a basic check for forward slashes, not full validation
  private static final String AWS_S3_FOLDER_NAME_REGEX = "/[^/]+";

  private static final Pattern AWS_S3_BUCKET_NAME_PATTERN =
      Pattern.compile(String.format("^%s$", AWS_S3_BUCKET_NAME_REGEX));
  private static final Pattern AWS_S3_ROOT_PATH_PATTERN =
      Pattern.compile(
          String.format("^/?(%s)(%s)*/?$", AWS_S3_BUCKET_NAME_REGEX, AWS_S3_FOLDER_NAME_REGEX));

  private static final String AZURE_STORAGE_ACCOUNT_REGEX = "[a-z0-9]{3,24}";
  private static final String AZURE_STORAGE_CONTAINER_REGEX = "[a-z0-9](?!.*--)[a-zA-Z0-9-]{2,62}";
  // Warning: only a basic check for forward slashes, not full validation
  private static final String AZURE_STORAGE_DIRECTORY_REGEX = "/[^/]+";

  private static final Pattern AZURE_STORAGE_ACCOUNT_PATTERN =
      Pattern.compile(String.format("^%s$", AZURE_STORAGE_ACCOUNT_REGEX));
  private static final Pattern AZURE_STORAGE_CONTAINER_PATTERN =
      Pattern.compile(String.format("^%s$", AZURE_STORAGE_CONTAINER_REGEX));
  private static final Pattern AZURE_ROOT_PATH_PATTERN =
      Pattern.compile(
          String.format(
              "^/?(%s)(%s)*/?$", AZURE_STORAGE_CONTAINER_REGEX, AZURE_STORAGE_DIRECTORY_REGEX));

  private CloudStoragePathValidator() {}

  /**
   *
   *
   * <pre>
   * Checks to see if a given bucket name is valid.
   *
   * AWS Bucket names are controlled by: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
   *
   * Bucket names must be between 3 (min) and 63 (max) characters long.
   * Bucket names can consist only of lowercase letters, numbers, dots (.), and hyphens (-).
   * Bucket names must begin and end with a letter or number.
   * Bucket names must not contain two adjacent periods.
   * (NOT CHECKED) Bucket names must not be formatted as an IP address (for example, 192.168.5.4).
   * (NOT CHECKED) Bucket names must not start with the prefix xn--.
   * (NOT CHECKED) Bucket names must not start with the prefix sthree- and the prefix sthree-configurator.
   * (NOT CHECKED) Bucket names must not end with the suffix -s3alias. This suffix is reserved for access point alias
   *   names. For more information, see Using a bucket-style alias for your S3 bucket access point.
   * (NOT CHECKED) Bucket names must not end with the suffix --ol-s3. This suffix is reserved for Object Lambda Access
   *   Point alias names. For more information, see How to use a bucket-style alias for your S3 bucket Object Lambda
   *   Access Point.
   * </pre>
   */
  public static boolean isValidAwsS3BucketName(String bucketName) {
    if (bucketName == null) {
      return false;
    }
    return AWS_S3_BUCKET_NAME_PATTERN.matcher(bucketName).find();
  }

  /**
   *
   *
   * <pre>
   * Checks to see if a given AWS S3 root path is valid.
   *
   * Root paths are a bucket name followed by an optional set of folder names
   * Folder name validation is a rudimentary check for forward slashes and may return valid for invalid folder names.
   * </pre>
   */
  public static boolean isValidAwsS3RootPath(String rootPath) {
    if (rootPath == null) {
      return false;
    }
    return AWS_S3_ROOT_PATH_PATTERN.matcher(rootPath).find();
  }

  /**
   *
   *
   * <pre>
   * Checks to see if a given storage account name is valid.
   *
   * Azure storage account names are controlled by:
   * https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview#storage-account-name
   *
   * Storage account names must be between 3 and 24 characters in length and may contain numbers and lowercase letters only.
   * </pre>
   */
  public static boolean isValidAzureStorageAccountName(String storageAccountName) {
    if (storageAccountName == null) {
      return false;
    }
    return AZURE_STORAGE_ACCOUNT_PATTERN.matcher(storageAccountName).find();
  }

  /**
   *
   *
   * <pre>
   * Checks to see if a given storage container name is valid.
   *
   * Azure storage container names are controlled by:
   * https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
   *
   * 3-63 characters.
   * Lowercase letters, numbers, and hyphens.
   * Start with lowercase letter or number.
   * Can't use consecutive hyphens.
   * </pre>
   */
  public static boolean isValidAzureStorageContainerName(String storageContainerName) {
    if (storageContainerName == null) {
      return false;
    }
    return AZURE_STORAGE_CONTAINER_PATTERN.matcher(storageContainerName).find();
  }

  /**
   *
   *
   * <pre>
   * Checks to see if a given Azure root path is valid.
   *
   * Root paths are a container name followed by an optional set of directory names
   * Directory name validation is a rudimentary check for forward slashes and may return valid for invalid directory names.
   *
   * See https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-shares--directories--files--and-metadata
   * for full information about valid Azure directory names.
   * </pre>
   */
  public static boolean isValidAzureStorageRootPath(String rootPath) {
    if (rootPath == null) {
      return false;
    }
    return AZURE_ROOT_PATH_PATTERN.matcher(rootPath).find();
  }
}
