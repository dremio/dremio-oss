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
export const BYTES_IN_KB = 1024;
export const BYTES_IN_MB = 1024 * 1024;
export const BYTES_IN_GB = 1024 * 1024 * 1024;

const FileUtils = {
  convertFileSize(size) {
    if (size < BYTES_IN_KB) {
      return '1.0KB';
    } else if (size < BYTES_IN_MB) {
      return Number(size / BYTES_IN_KB).toFixed(1) + 'KB';
    } else if (size < BYTES_IN_GB) {
      return Number(size / BYTES_IN_MB).toFixed(1) + 'MB';
    }
    return Number(size / BYTES_IN_GB).toFixed(1) + 'GB';
  }
};

export default FileUtils;
