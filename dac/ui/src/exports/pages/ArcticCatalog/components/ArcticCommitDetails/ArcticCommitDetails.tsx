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

import { WithRouterProps } from "react-router";
import ArcticCommitDetailsHeader from "./components/ArcticCommitDetailsHeader/ArcticCommitDetailsHeader";
import ArcticCommitDetailsBody from "./components/ArcticCommitDetailsBody/ArcticCommitDetailsBody";
import { Spinner } from "dremio-ui-lib/dist-esm";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { ArcticCatalogTabsType } from "@app/exports/pages/ArcticCatalog/ArcticCatalog";
import { useArcticCommitDetails } from "./useArcticCommitDetails";
import { useArcticCommitTags } from "./useArcticCommitTags";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";

import * as classes from "./ArcticCommitDetails.module.less";

type ArcticCommitDetailsProps = WithRouterProps;

function ArcticCommitDetails({ router, params }: ArcticCommitDetailsProps) {
  const {
    state: { reference, hash },
    baseUrl,
  } = useNessieContext();
  const { reservedNamespace, isCatalog } = useArcticCatalogContext() ?? {};

  const getPath = (tab: ArcticCatalogTabsType) =>
    constructArcticUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: reservedNamespace ?? "",
      hash: hash ? `?hash=${hash}` : "",
    });

  const handlePush = (tab: ArcticCatalogTabsType) => router.push(getPath(tab));

  const [data, , status] = useArcticCommitDetails(
    params.commitId,
    reference?.name
  );

  const [tags, , tagStatus, refetch] = useArcticCommitTags(params.commitId);

  return (
    <div className={classes["arctic-commit-details"]}>
      <ArcticCommitDetailsHeader
        commitId={params.commitId}
        reference={reference}
        handlePush={handlePush}
      />
      {isSmartFetchLoading(status) || isSmartFetchLoading(tagStatus) ? (
        <Spinner className={classes["arctic-commit-details__spinner"]} />
      ) : (
        <ArcticCommitDetailsBody
          references={tags?.references ?? []}
          commitId={params.commitId}
          commitData={data?.logEntries?.[0]}
          refetch={refetch}
        />
      )}
    </div>
  );
}

export default ArcticCommitDetails;
