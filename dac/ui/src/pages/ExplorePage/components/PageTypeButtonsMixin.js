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
import { PageTypes } from "@app/pages/ExplorePage/pageTypes";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";

export default function (input) {
  const originalFn = input.prototype.getAvailablePageTypes;

  if (!originalFn) {
    throw new Error("Input must have getAvailablePageTypes method");
  }

  Object.assign(input.prototype, {
    // eslint-disable-line no-restricted-properties
    getAvailablePageTypes() {
      const { dataset, showWiki } = this.props;
      const pageTypeList = originalFn.call(this);

      const versionedContextForDataset = getVersionContextFromId(
        dataset.get("entityId")
      );
      if (showWiki && !versionedContextForDataset) {
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
    },
  });
}
