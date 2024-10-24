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
import { defaultPageTypeList, useShowWiki } from "./explorePageTypeUtils";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

export const getAvailablePageTypes = (args: {
  dataset: any;
  showWiki: boolean;
  supportFlags?: Record<string, string>;
}) => {
  const { dataset, showWiki } = args;
  const pageTypeList = [...defaultPageTypeList];

  const versionedContextForDataset = getVersionContextFromId(
    dataset.get("entityId"),
  );

  if (showWiki) {
    pageTypeList.push(PageTypes.wiki);
  }
  const isNewQuery =
    dataset.get("isNewQuery") ||
    !dataset.getIn(["apiLinks", "namespaceEntity"]);

  if (!isNewQuery) {
    pageTypeList.push(PageTypes.reflections);
  }

  if (versionedContextForDataset) {
    pageTypeList.push(PageTypes.history);
  }

  return pageTypeList;
};

export const withAvailablePageTypes =
  <T,>(WrappedComponent: React.ComponentClass) =>
  (props: T & { dataset: any }) => {
    return (
      <WrappedComponent
        availablePageTypes={getAvailablePageTypes({
          dataset: props.dataset,
          showWiki: useShowWiki(),
        })}
        {...props}
      />
    );
  };
