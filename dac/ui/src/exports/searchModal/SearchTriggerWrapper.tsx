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

import { type FC, type ReactElement, Suspense } from "react";
import { SupportKeyValue } from "#oss/exports/providers/useSupportKeyValue";
import { CatalogSearchModalTrigger } from "dremio-ui-common/sonar/CatalogSearch/CatalogSearchModalTrigger.js";
import { useSearchModal } from "dremio-ui-common/sonar/CatalogSearch/CatalogSearchModalContext.js";

const SearchModalProp: FC<{
  children: (
    ctx: ReturnType<typeof useSearchModal>,
  ) => ReactElement<any, any> | null;
}> = (props) => props.children(useSearchModal());

export const SearchTriggerWrapper: FC<{ className?: string }> = (props) => {
  return (
    <Suspense fallback={null}>
      <SupportKeyValue
        supportKey="nextgen_search.ui.enable"
        defaultValue={false}
      >
        {(enabled) => {
          if (!enabled) {
            return null;
          }
          return (
            <div {...props} style={{ maxWidth: "280px", flex: "1" }}>
              <SearchModalProp>
                {(ctx) => <CatalogSearchModalTrigger onOpen={ctx.open} />}
              </SearchModalProp>
            </div>
          );
        }}
      </SupportKeyValue>
    </Suspense>
  );
};
