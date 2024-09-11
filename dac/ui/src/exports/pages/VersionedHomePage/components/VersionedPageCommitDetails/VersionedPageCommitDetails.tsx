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
import VersionedCommitDetailsHeader from "./components/VersionedCommitDetailsHeader/VersionedCommitDetailsHeader";
import VersionedCommitDetailsBody from "./components/VersionedCommitDetailsBody/VersionedCommitDetailsBody";
import { Spinner } from "dremio-ui-lib/components";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import { useVersionedCommitDetails } from "./useVersionedCommitDetails";
import { useVersionedCommitTags } from "./useVersionedCommitTags";
import { isSmartFetchLoading } from "@app/utils/isSmartFetchLoading";
import { useDispatch } from "react-redux";
import { setReference } from "@app/actions/nessie/nessie";

import * as classes from "./VersionedPageCommitDetails.module.less";

type VersionedPageCommitDetailsProps = WithRouterProps;

function VersionedPageCommitDetails({
  router,
  params,
}: VersionedPageCommitDetailsProps) {
  const {
    state: { reference, hash },
    stateKey,
    baseUrl,
  } = useNessieContext();
  const { reservedNamespace, isCatalog } = useVersionedPageContext();
  const dispatch = useDispatch();

  const getPath = (tab: VersionedPageTabsType) => {
    const toHash = tab === "data" ? params.commitId : hash;
    let toNamespace = reservedNamespace ?? "";
    if (!toNamespace.includes(params.branchName)) {
      toNamespace = encodeURIComponent(params.branchName);
    }

    return constructVersionedEntityUrl({
      type: isCatalog ? "catalog" : "source",
      baseUrl,
      tab,
      namespace: toNamespace,
      hash: toHash ? `?hash=${toHash}` : "",
    });
  };

  const handleReference = (tab: VersionedPageTabsType) => {
    if (tab === "data" && reference?.name) {
      dispatch(
        setReference(
          {
            reference: {
              name: reference.name,
              hash: params.commitId,
              type: "BRANCH",
            },
            hash: params.commitId,
          },
          stateKey,
        ),
      );
    }
  };

  const handleTabNavigation = (tab: VersionedPageTabsType) => {
    handleReference(tab);
    return router.push(getPath(tab));
  };

  const [data, , status] = useVersionedCommitDetails(
    params.commitId,
    params.branchName,
  );

  const [tags, , tagStatus, refetch] = useVersionedCommitTags(params.commitId);

  return (
    <div className={classes["versioned-commit-details"]}>
      <VersionedCommitDetailsHeader
        commitId={params.commitId}
        reference={reference}
        handleTabNavigation={handleTabNavigation}
      />
      {isSmartFetchLoading(status) || isSmartFetchLoading(tagStatus) ? (
        <Spinner className={classes["versioned-commit-details__spinner"]} />
      ) : (
        <VersionedCommitDetailsBody
          references={tags?.references ?? []}
          commitId={params.commitId}
          commitData={data?.logEntries?.[0]}
          refetch={refetch}
        />
      )}
    </div>
  );
}

export default VersionedPageCommitDetails;
