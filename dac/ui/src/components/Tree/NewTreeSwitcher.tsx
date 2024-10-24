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

import { type FC } from "react";
import { getSonarContext } from "dremio-ui-common/contexts/SonarContext.js";
import { CatalogTree } from "#oss/exports/components/CatalogTree/CatalogTree";
import { useQuery } from "@tanstack/react-query";
import { catalogRootQuery } from "@inject/queries/catalog";
import { ifDev } from "#oss/exports/utilities/ifDev";
import { SectionMessage, Spinner } from "dremio-ui-lib/components";

export const NewTreeSwitcher: FC<{
  oldRenderHome: () => JSX.Element | undefined;
  oldRenderItems: () => JSX.Element | JSX.Element[] | undefined;
}> = (props) => {
  const catalogReferences = useQuery(
    catalogRootQuery(getSonarContext().getSelectedProjectId?.()),
  );

  return (
    ifDev(
      catalogReferences.isPending ? (
        <Spinner className="flex flex-row items-center justify-center h-10" />
      ) : catalogReferences.isError ? (
        <SectionMessage appearance="danger" className="mx-105 mt-05">
          {`Error: ${catalogReferences.error.message}`}
        </SectionMessage>
      ) : (
        <CatalogTree
          catalogReferences={catalogReferences.data
            .filter((catalogReference) => catalogReference.type !== "FUNCTION")
            .sort((a, b) => a.name.localeCompare(b.name))}
        />
      ),
    ) ?? (
      <div className="TreeBrowser-items">
        {props.oldRenderHome()}
        {props.oldRenderItems()}
      </div>
    )
  );
};
