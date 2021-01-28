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


import APICallMixin from '@inject/core/APICallMixin';
/**
 * APICall handles building urls for API calls.
 */
@APICallMixin
export default class APICall {
  _path = [];
  _fullPath = null;
  _params = new Map();
  _apiVersion = 3;
  _appendFrontSlash = true;

  apiVersion(value) {
    this._apiVersion = value;
    return this;
  }

  // treats value as a single path
  path(value) {
    this._path.push(value);

    return this;
  }

  // supports multiple paths as a string: /foo/bar/baz
  paths(value) {
    if (value) {
      const paths = value.split('/')
        .filter((path) => {
          return !!path;
        });
      this._path = this._path.concat(paths);
    }

    return this;
  }

  // set the full path, overriding any parsing/processing.  Only to be used for things like server generated urls.
  fullpath(fullpath) {
    this._fullPath = fullpath;
    return this;
  }

  param(key, value) {
    this._params.set(key, value);

    return this;
  }

  params(params) {
    for (const [key, value] of Object.entries(params)) {
      this.param(key, value);
    }

    return this;
  }

  appendFrontSlash(value = true) {
    this._appendFrontSlash = value;
    return this;
  }

  uncachable() {
    this._params.set('nocache', Date.now());
    return this;
  }

  /**
   * Returns the path as a string.  Does not include the api version!
   */
  toPath() {
    let url = '';

    if (this._fullPath) {
      url = this._fullPath;
    } else if (this._path) {
      url = this._path.map((path) => {
        // handle already encoded paths - mainly from api links on the server.
        return encodeURIComponent(decodeURIComponent(path));
      }).join('/');
    }

    if (!url.startsWith('/')) {
      url = '/' + url;
    }

    if (this._params.size === 0) {
      return url;
    }

    const urlSearchParams = new URLSearchParams();

    for (const [key, value] of this._params.entries()) {
      if (Array.isArray(value)) {
        value.forEach((v) => {
          urlSearchParams.append(key, v);
        });
      } else {
        urlSearchParams.set(key, value);
      }
    }

    // url may contain ? due to fullpath, so handle that
    if (url.indexOf('?') > -1) {
      return url + '&' + urlSearchParams.toString();
    } else {
      // require a trailing / when we have parameters
      if (!url.endsWith('/') && this._appendFrontSlash) {
        url += '/';
      }

      return url + '?' + urlSearchParams.toString();
    }
  }

  /**
   * Returns the full url as a string.
   */
  toString() {
    let url = this.getUrl(this._apiVersion);
    url += this.toPath();

    return url;
  }
}

// v2 API shortcut for APICall
export class APIV2Call extends APICall {
  constructor() {
    super();
    super.apiVersion(2);
  }

  apiVersion() {
    return this;
  }
}
