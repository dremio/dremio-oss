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

import type { FC } from "react";
import { getIntlContext } from "../../contexts/IntlContext";

export const RecentSearches: FC<{
  searches: string[];
  onSearchClicked: (search: string) => void;
}> = (props) => {
  const { t } = getIntlContext();
  return (
    <div className="mt-2 overflow-y-auto">
      <div className="text-semibold dremio-typography-less-important dremio-typography-small">
        {t("Catalog.Search.RecentSearches.Title")}
      </div>
      <ul className="mt-05">
        {props.searches.map((search, i) => (
          <li key={i} className="mt-05">
            <button
              className="bg-primary w-full flex items-center hover p-1"
              onClick={() => props.onSearchClicked(search)}
              type="button"
              style={{
                border: "none",
                borderRadius: "4px",
                transition: "background 100ms",
              }}
            >
              <div className="dremio-icon-label overflow-hidden">
                <dremio-icon
                  name="column-types/time"
                  style={{
                    width: "20px",
                    height: "20px",
                    color: "var(--icon--primary)",
                  }}
                ></dremio-icon>
                <span className="truncate" title={search}>
                  {search}
                </span>
              </div>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
};
