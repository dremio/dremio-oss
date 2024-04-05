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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestCloudStoragePathValidator {

  private static final List<Arguments> folderOrDirectorySuffixArguments =
      Arrays.asList(
          // Valid
          Arguments.of("", true), // Storage container only
          Arguments.of("someDirectory", true),
          Arguments.of("someDirectoryEndingInSlash/", true),
          Arguments.of("multiple/directories", true),
          Arguments.of("multiple/directories/ending/in/slash/", true),

          // Invalid
          Arguments.of("//", false),
          Arguments.of("multiple//directoriesWithDoubleSlash", false));

  private static Stream<Arguments> awsS3BucketNameValidity() {
    return Stream.of(
        // Valid
        Arguments.of("aaa", true), // Minimum number of characters
        Arguments.of(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa63",
            true), // Maximum number of characters
        Arguments.of("alllowercase", true),
        Arguments.of("1startswithnumber", true),
        Arguments.of("number1inmiddle", true),
        Arguments.of("numberatend1", true),
        Arguments.of("has11many1numbers", true),
        Arguments.of("1has11many1numbers1", true),
        Arguments.of("hyphen-inmiddle", true),
        Arguments.of("dot.inmiddle", true),
        Arguments.of("eachnumber0123456789", true),
        Arguments.of("abcdefghijklmnopqrstuvwxyz", true),

        // Invalid
        Arguments.of("aa", false), // Too few characters
        Arguments.of("toomanycharacterstoomanycharacterstoomanycharacterstoomanychar64", false),
        Arguments.of("Startswithcapital", false),
        Arguments.of("capitalInmiddle", false),
        Arguments.of("endswithcapitaL", false),
        Arguments.of("-startsWithHyphen", false),
        Arguments.of("hyphenatend-", false),
        Arguments.of(".startswithdot", false),
        Arguments.of("endswithdot.", false),
        Arguments.of("_startswithunderscore", false),
        Arguments.of("underscore_inmiddle", false),
        Arguments.of("underscoreatend_", false),
        Arguments.of("!startswithsymbol", false),
        Arguments.of("symbol!inmiddle", false),
        Arguments.of("endswithsymbol!", false),
        Arguments.of("\\startswithbackslash", false),
        Arguments.of("backslash\\inmiddle", false),
        Arguments.of("endswithbackslash\\", false),
        Arguments.of(" startswithspace", false),
        Arguments.of("space inmiddle", false),
        Arguments.of("endswithspace ", false),
        Arguments.of("\"startswithdoublequote", false),
        Arguments.of("doublequote\"inmiddle", false),
        Arguments.of("endswithdoublequote\"", false),
        Arguments.of("'startswithsinglequote", false),
        Arguments.of("singlequote'inmiddle", false),
        Arguments.of("endswithsinglequote'", false),
        Arguments.of("..consecutivedotsatstart", false),
        Arguments.of("consecutive..dotsinmiddle", false),
        Arguments.of("consecutivedotsatend..", false));
  }

  @ParameterizedTest
  @MethodSource("awsS3BucketNameValidity")
  void testIsValidAwsS3Bucket(String bucketName, boolean isValid) {
    assertThat(CloudStoragePathValidator.isValidAwsS3BucketName(bucketName)).isEqualTo(isValid);
  }

  private static Stream<Arguments> testIsValidAwsS3RootPathParams() {
    // Cartesian product of bucket names with directory suffixes
    return awsS3BucketNameValidity()
        .flatMap(
            storageContainerNameArgument ->
                folderOrDirectorySuffixArguments.stream()
                    .flatMap(
                        directorySuffixArgument -> {
                          final String storageContainerName =
                              (String) storageContainerNameArgument.get()[0];
                          final boolean storageContainerNameIsValid =
                              (boolean) storageContainerNameArgument.get()[1];
                          final String directorySuffix = (String) directorySuffixArgument.get()[0];
                          final boolean directorySuffixIsValid =
                              (boolean) directorySuffixArgument.get()[1];

                          final String rootPath =
                              String.format("/%s/%s", storageContainerName, directorySuffix);
                          final boolean isValid =
                              storageContainerNameIsValid && directorySuffixIsValid;
                          return Stream.of(Arguments.of(rootPath, isValid));
                        }));
  }

  @ParameterizedTest
  @MethodSource("testIsValidAwsS3RootPathParams")
  void testIsValidAwsS3RootPath(String awsRootPath, boolean isValid) {
    assertThat(CloudStoragePathValidator.isValidAwsS3RootPath(awsRootPath)).isEqualTo(isValid);
  }

  private static Stream<Arguments> azureStorageAccountNameValidity() {
    return Stream.of(
        // Valid
        Arguments.of("aaa", true), // Minimum number of characters
        Arguments.of("alllowercase", true),
        Arguments.of("1startswithnumber", true),
        Arguments.of("number1inmiddle", true),
        Arguments.of("numberatend1", true),
        Arguments.of("has11many1numbers", true),
        Arguments.of("1has11many1numbers1", true),
        Arguments.of("eachnumber0123456789", true),
        Arguments.of("abcdefghijklmnopqrstuvwx", true),
        Arguments.of("cdefghijklmnopqrstuvwxyz", true),

        // Invalid
        Arguments.of("aa", false), // Too few characters
        Arguments.of("toomanycharacterstooman25", false),
        Arguments.of("Startswithcapital", false),
        Arguments.of("capitalInmiddle", false),
        Arguments.of("endswithcapitaL", false),
        Arguments.of("-startsWithHyphen", false),
        Arguments.of("hyphen-InMiddle", false),
        Arguments.of("hyphenatend-", false),
        Arguments.of("_startswithunderscore", false),
        Arguments.of("underscore_inmiddle", false),
        Arguments.of("underscoreatend_", false),
        Arguments.of("!startswithsymbol", false),
        Arguments.of("symbol!inmiddle", false),
        Arguments.of("endswithsymbol!", false),
        Arguments.of("\\startswithbackslash", false),
        Arguments.of("backslash\\inmiddle", false),
        Arguments.of("endswithbackslash\\", false),
        Arguments.of(" startswithspace", false),
        Arguments.of("space inmiddle", false),
        Arguments.of("endswithspace ", false),
        Arguments.of("\"startswithdoublequote", false),
        Arguments.of("doublequote\"inmiddle", false),
        Arguments.of("endswithdoublequote\"", false),
        Arguments.of("'startswithsinglequote", false),
        Arguments.of("singlequote'inmiddle", false),
        Arguments.of("endswithsinglequote'", false));
  }

  @ParameterizedTest
  @MethodSource("azureStorageAccountNameValidity")
  void testIsValidAzureStorageAccountName(String storageAccountName, boolean isValid) {
    assertThat(CloudStoragePathValidator.isValidAzureStorageAccountName(storageAccountName))
        .isEqualTo(isValid);
  }

  private static Stream<Arguments> azureStorageContainerNameValidity() {
    return Stream.of(
        // Valid
        Arguments.of("aaa", true), // Minimum number of characters
        Arguments.of("startsWithLowercase", true),
        Arguments.of("1startsWithNumber", true),
        Arguments.of("hasNumber1InMiddle", true),
        Arguments.of("hasNumberAtEnd1", true),
        Arguments.of("has11Many1Numbers", true),
        Arguments.of("1has11Many1Numbers1", true),
        Arguments.of("hyphen-InMiddle", true),
        Arguments.of("hyphenAtEnd-", true),
        Arguments.of("capitalInMiddle", true),
        Arguments.of("endsWithCapitaL", true),
        Arguments.of("hasEachNumber0123456789", true),
        Arguments.of("hasEachLetterabcdefghijklmnopqrstuvwxyz", true),
        Arguments.of("hasEachLetterABCDEFGHJIKLMNOPQRSTUVWXYZ", true),

        // Invalid
        Arguments.of("aa", false), // Too few characters
        Arguments.of("tooManyCharactersTooManyCharactersTooManyCharactersTooManyChar64", false),
        Arguments.of("StartsWithCapital", false),
        Arguments.of("-startsWithHyphen", false),
        Arguments.of("_startsWithUnderscore", false),
        Arguments.of("underscore_InMiddle", false),
        Arguments.of("underscoreAtEnd_", false),
        Arguments.of("!startsWithSymbol", false),
        Arguments.of("symbol!InMiddle", false),
        Arguments.of("endsWithSymbol!", false),
        Arguments.of("\\startsWithBackslash", false),
        Arguments.of("backslash\\InMiddle", false),
        Arguments.of("endsWithBackslash\\", false),
        Arguments.of(" startsWithSpace", false),
        Arguments.of("space InMiddle", false),
        Arguments.of("endsWithSpace ", false),
        Arguments.of("\"startsWithDoubleQuote", false),
        Arguments.of("doublequote\"InMiddle", false),
        Arguments.of("endsWithDoubleQuote\"", false),
        Arguments.of("'startsWithSingleQuote", false),
        Arguments.of("singlequote'InMiddle", false),
        Arguments.of("endsWithSingleQuote'", false),
        Arguments.of("--consecutiveHyphensAtStart", false),
        Arguments.of("consecutive--HyphensInMiddle", false),
        Arguments.of("consecutiveHyphensAtEnd--", false));
  }

  @ParameterizedTest
  @MethodSource("azureStorageContainerNameValidity")
  void testIsValidAzureStorageContainerName(String storageContainerName, boolean isValid) {
    assertThat(CloudStoragePathValidator.isValidAzureStorageContainerName(storageContainerName))
        .isEqualTo(isValid);
  }

  private static Stream<Arguments> testIsValidAzureStorageRootPathParams() {
    // Cartesian product of storage container names with directory suffixes
    return azureStorageContainerNameValidity()
        .flatMap(
            storageContainerNameArgument ->
                folderOrDirectorySuffixArguments.stream()
                    .flatMap(
                        directorySuffixArgument -> {
                          final String storageContainerName =
                              (String) storageContainerNameArgument.get()[0];
                          final boolean storageContainerNameIsValid =
                              (boolean) storageContainerNameArgument.get()[1];
                          final String directorySuffix = (String) directorySuffixArgument.get()[0];
                          final boolean directorySuffixIsValid =
                              (boolean) directorySuffixArgument.get()[1];

                          final String rootPath =
                              String.format("/%s/%s", storageContainerName, directorySuffix);
                          final boolean isValid =
                              storageContainerNameIsValid && directorySuffixIsValid;
                          return Stream.of(Arguments.of(rootPath, isValid));
                        }));
  }

  @ParameterizedTest
  @MethodSource("testIsValidAzureStorageRootPathParams")
  void testIsValidAzureStorageRootPath(String rootPath, boolean isValid) {
    assertThat(CloudStoragePathValidator.isValidAzureStorageRootPath(rootPath)).isEqualTo(isValid);
  }

  @Test
  public void testIsValidAzureStorageRootPathWithNullRootPath() {
    assertThat(CloudStoragePathValidator.isValidAzureStorageRootPath(null)).isEqualTo(false);
  }

  @Test
  public void testIsValidAWSS3RootPathWithNullRootPath() {
    assertThat(CloudStoragePathValidator.isValidAwsS3RootPath(null)).isEqualTo(false);
  }

  @Test
  public void testisValidAwsS3BucketNameWithNullBucketName() {
    assertThat(CloudStoragePathValidator.isValidAwsS3BucketName(null)).isEqualTo(false);
  }

  @Test
  public void testisValidAzureStorageAccountNameWithNullAccountName() {
    assertThat(CloudStoragePathValidator.isValidAzureStorageAccountName(null)).isEqualTo(false);
  }

  @Test
  public void testisValidAzureStorageContainerNameWithNullContainerName() {
    assertThat(CloudStoragePathValidator.isValidAzureStorageContainerName(null)).isEqualTo(false);
  }
}
