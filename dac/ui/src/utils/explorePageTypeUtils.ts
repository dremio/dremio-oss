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

import { PageTypes } from "#oss/pages/ExplorePage/pageTypes";
import { isWikAvailable } from "#oss/selectors/explore";
import { rmProjectBase } from "dremio-ui-common/utilities/projectBase.js";
import { useSelector } from "react-redux";
import { browserHistory } from "react-router";

export const defaultPageTypeList = [PageTypes.default];

export const useShowWiki = () => {
  return useSelector((state: any) =>
    isWikAvailable(state, browserHistory.getCurrentLocation()),
  );
};

export const isNewQueryUrl = (location: any) => {
  const { query = {}, pathname } = location;
  return (
    !query.tipVersion &&
    !query.version &&
    rmProjectBase(pathname).startsWith("/new_query")
  );
};

export const isTmpDatasetUrl = (location: any) => {
  const { query = {}, pathname } = location;
  return (
    (query.tipVersion || query.version) &&
    rmProjectBase(pathname).startsWith("/new_query")
  );
};

export const isTabbableUrl = (location: any) => {
  return rmProjectBase(location.pathname) === "/new_query";
};

export const isScriptUrl = (location: any) => {
  const { query = {} } = location;

  return isTabbableUrl(location) && !!query.scriptId;
};
