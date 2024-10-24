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

import { FC } from "react";
import { getIntlContext } from "../../contexts/IntlContext";
import { noResultsImg } from "./noResultsImg";

export const CatalogSearchUnexpectedError: FC<{ searchText: string }> = (
  props,
) => {
  const { t } = getIntlContext();
  return (
    <div className="flex flex-col gap-1 items-center overflow-x-hidden overflow-y-auto p-2 text-center">
      {noResultsImg}
      <p className="text-semibold text-lg" style={{ wordBreak: "break-all" }}>
        {t("Catalog.Search.NoResultsFound.Title", {
          searchText: props.searchText,
        })}
      </p>
      <p className="dremio-typography-less-important">
        {t("Catalog.Search.UnexpectedError")}
      </p>
    </div>
  );
};
