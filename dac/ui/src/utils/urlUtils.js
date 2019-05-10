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
import invariant from 'invariant';

/**
 * Adds query parameter to a {@see url}
 *
 * @param {string!} url
 * @param {string!} paramName - query parameter name
 * @param {string!} value - parameter value
 */
export const addParameterToUrl = (url, paramName, value) => {
  invariant(url, 'url must be defined');
  invariant(paramName, 'paramName must be defined');
  invariant(value, 'value must be defined');
  return `${url}${url.includes('?') ? '&' : '?'}${encodeURIComponent(paramName)}=${encodeURIComponent(value)}`;
};
