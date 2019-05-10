/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.exec.store.hive.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for HiveMetadataUtils
 */
public class TestHiveMetadataUtils {

  FileInputFormat mock_fileInputFormat = mock(FileInputFormat.class);
  OrcInputFormat mock_orcInputFormat = mock(OrcInputFormat.class);
  InputFormat mock_inputFormat = mock(InputFormat.class);

  final StorageDescriptor storageDescriptor_s3 = new StorageDescriptor();
  final StorageDescriptor storageDescriptor_s3a = new StorageDescriptor();
  final StorageDescriptor storageDescriptor_hdfs = new StorageDescriptor();
  final StorageDescriptor storageDescriptor_empty = new StorageDescriptor();
  final StorageDescriptor storageDescriptor_invalid_uri = new StorageDescriptor();

  @Before
  public void init() {
    storageDescriptor_s3.setLocation("s3://somehost/somepath");
    storageDescriptor_s3a.setLocation("s3a://somehost/somepath");
    storageDescriptor_hdfs.setLocation("hdfs://somehost/somepath");
    storageDescriptor_invalid_uri.setLocation("123and_thensomeother_nonsense\\over here.");
  }

  @Test
  public void getHiveTableCapabilities_s3_supportsImpersonation() {
    assertFalse(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3).supportsImpersonation());
  }

  @Test
  public void getHiveTableCapabilities_s3_supportsLastModifiedTime() {
    assertFalse(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3).supportsLastModifiedTime());
  }

  @Test
  public void getHiveTableCapabilities_s3a_supportsImpersonation() {
    assertFalse(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3a).supportsImpersonation());
  }

  @Test
  public void getHiveTableCapabilities_s3a_supportsLastModifiedTime() {
    assertFalse(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3a).supportsLastModifiedTime());
  }

  @Test
  public void getHiveTableCapabilities_hdfs_supportsImpersonation() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs).supportsImpersonation());
  }

  @Test
  public void getHiveTableCapabilities_hdfs_supportsLastModifiedTime() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs).supportsLastModifiedTime());
  }

  @Test
  public void getHiveTableCapabilities_unknownLocation_supportsImpersonation() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_empty).supportsImpersonation());
  }

  @Test
  public void getHiveTableCapabilities_unknownLocation_supportsLastModifiedTime() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_empty).supportsLastModifiedTime());
  }

  @Test
  public void getHiveTableCapabilities_invalid_uri_supportsImpersonation() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_invalid_uri).supportsImpersonation());
  }

  @Test
  public void getHiveTableCapabilities_invalid_uri_supportsLastModifiedTime() {
    assertTrue(HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_invalid_uri).supportsLastModifiedTime());
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_s3_no_impersonation() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3),
        false));
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_s3_impersonation() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3),
        true));
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_s3a_no_impersonation() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3a),
        false));
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_s3a_impersonation() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3a),
        true));
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_hdfs_no_impersonation() {
    assertTrue(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs),
        false));
  }

  @Test
  public void shouldGenerateFSUKeysForDirectoriesOnly_hdfs_impersonation() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFSUKeysForDirectoriesOnly(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs),
        true));
  }

  @Test
  public void shouldGenerateFileSystemUpdateKeys_s3_fileinputformat() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3),
        mock_fileInputFormat));
  }

  @Test
  public void shouldGenerateFileSystemUpdateKeys_s3_orcinputformat() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3),
        mock_orcInputFormat));
  }

  @Test
  public void shouldGenerateFileSystemUpdateKeysS3_s3_inputformat() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_s3),
        mock_inputFormat));
  }


  @Test
  public void shouldGenerateFileSystemUpdateKeys_hdfs_fileinputformat() {
    assertTrue(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs),
        mock_fileInputFormat));
  }

  @Test
  public void shouldGenerateFileSystemUpdateKeys_s3_hdfs_orcinputformat() {
    assertTrue(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs),
        mock_orcInputFormat));
  }

  @Test
  public void shouldGenerateFileSystemUpdateKeys_s3_hdfs_inputformat() {
    assertFalse(HiveMetadataUtils
      .shouldGenerateFileSystemUpdateKeys(
        HiveMetadataUtils.getHiveStorageCapabilities(storageDescriptor_hdfs),
        mock_inputFormat));
  }
}
