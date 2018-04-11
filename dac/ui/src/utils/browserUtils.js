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
import platform from 'platform';
import localStorageUtils from './storageUtils/localStorageUtils';

const oldestSupportedBrowserVersions = {
  'Chrome': '54',
  'Firefox': '50',
  'IE': '11',
  'Microsoft Edge': '14',
  'Safari': '11'
};

const browserUtils = {
  getPlatform() { // wrap for easy test stubbing
    return platform;
  },
  compareVersions(a, b) {
    const first = a.split('.');
    const second = b.split('.');
    const minLength = Math.min(first.length, second.length);
    let result;
    for (let i = 0; i < minLength; i++) {
      result = parseInt(first[i], 10) - parseInt(second[i], 10);
      if (result !== 0) {
        return result;
      }
    }

    return first.length - second.length;
  },
  hasSupportedBrowserVersion() {
    const { name, version } = this.getPlatform();
    const supportedVersion = oldestSupportedBrowserVersions[name];

    if (!supportedVersion) {
      return false;
    }

    return this.compareVersions(version, supportedVersion) >= 0;
  },
  approveUnsupportedBrowser() {
    localStorageUtils.setCustomValue('isApprovedUnsupportedBrowser', true);
  },
  isApprovedUnsupportedBrowser() {
    return localStorageUtils.getCustomValue('isApprovedUnsupportedBrowser');
  },
  isMSBrowser() {
    return ['IE', 'Microsoft Edge'].includes(platform.name);
  }
};

export default browserUtils;
