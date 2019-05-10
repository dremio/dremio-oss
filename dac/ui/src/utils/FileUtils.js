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
import Immutable from 'immutable';

import localStorageUtils from 'utils/storageUtils/localStorageUtils';
import FileSaver from 'file-saver';

class FileDownloadError {
  constructor(message) {
    this.message = message;
  }
}

const STEP = 1024;
const BYTES_IN_KILOBYTE = STEP;
const BYTES_IN_MEGABYTE = STEP * BYTES_IN_KILOBYTE;
const BYTES_IN_GIGABYTE = STEP * BYTES_IN_MEGABYTE;
const BYTES_IN_TERABYTE = STEP * BYTES_IN_GIGABYTE;


export default class FileUtils {
  static downloadFile({ blob, fileName }) {
    // works for FF, Chrome, IE
    FileSaver.saveAs(blob, fileName);
  }

  static getFileNameFromResponse(response) {
    const contentDisposition = response.headers.get('Content-Disposition');
    if (contentDisposition) {
      const m = contentDisposition.match(/attachment; filename=\"([^"]+)\"/);
      if (m) {
        return m[1];
      }
    }
    return 'download';
  }

  static getFileDownloadConfigFromResponse(response) {

    const defaultError = new FileDownloadError('Download failed: ' + response.statusText);
    if (!response.ok) {
      return response.json().then((body) => {
        if (body.errorMessage) {
          throw new FileDownloadError(Immutable.fromJS(body));
        }
        throw defaultError;
      }).catch((e) => {
        // .json() failed?
        if (!(e instanceof FileDownloadError)) {
          throw defaultError;
        }
        throw e;
      });
    }

    const emptyCodes = new Set([204, 205]);
    if (emptyCodes.has(response.status)) {
      throw new FileDownloadError('File has no contents');
    }
    const fileName = this.getFileNameFromResponse(response);
    return response.blob().then(blob => ({blob, fileName}));
  }

  static getHeaders() {
    const headers = new Headers();
    headers.append('Accept', '*');
    const authToken = localStorageUtils && localStorageUtils.getAuthToken();
    if (authToken) {
      headers.append('Authorization', authToken);
    }
    return headers;
  }

  static getFormattedBytes(bytes) {
    if ((!bytes && bytes !== 0) || isNaN(bytes) || isNaN(Number(bytes))) {
      return '';
    }
    if (bytes < BYTES_IN_KILOBYTE) {
      return `${bytes} B`;
    } else if (bytes < BYTES_IN_MEGABYTE) {
      return `${(bytes / BYTES_IN_KILOBYTE).toFixed(2)} KB`;
    } else if (bytes < BYTES_IN_GIGABYTE) {
      return `${(bytes / BYTES_IN_MEGABYTE).toFixed(2)} MB`;
    } else if (bytes < BYTES_IN_TERABYTE) {
      return `${(bytes / BYTES_IN_GIGABYTE).toFixed(2)} GB`;
    }

    return `${(bytes / BYTES_IN_TERABYTE).toFixed(2)} TB`;
  }

}
