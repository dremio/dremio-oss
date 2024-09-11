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

import { useContext, useId } from "react";
import { FormattedMessage } from "react-intl";
import { withRouter, WithRouterProps } from "react-router";
import { connect } from "react-redux";

import CommitHash from "@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { Avatar, Button } from "dremio-ui-lib/components";
import PromiseViewState from "@app/components/PromiseViewState/PromiseViewState";
import { Reference } from "@app/types/nessie";
import { RepoViewContext } from "../../../../RepoView";
import { convertISOStringWithTooltip, renderIcons } from "./utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { setReference } from "@app/actions/nessie/nessie";
import { stopPropagation } from "@app/utils/reactEventUtils";
import EmptyStateContainer from "@app/pages/HomePage/components/EmptyStateContainer";
import { useResourceSnapshot } from "smart-resource/react";
import { ArcticCatalogPrivilegesResource } from "@inject/arctic/resources/ArcticCatalogPrivilegesResource";
import { CatalogPrivilegeSwitch } from "@app/exports/components/CatalogPrivilegeSwitch/CatalogPrivilegeSwitch";
import { SmartResource } from "smart-resource";

import "./RepoViewBranchList.less";

type RepoViewBranchTableProps = {
  rows: Reference[];
  openCreateDialog: (branch: Reference, isDefault?: boolean) => void;
  openTagDialog?: (branch: Reference, isDefault?: boolean) => void;
  openDeleteDialog?: (branch: Reference) => void;
  openMergeDialog?: (branch: Reference) => void;
  isDefault?: boolean;
  setReference: typeof setReference;
  isArcticSource: boolean;
  defaultReference?: Reference;
  noSearchResults?: boolean;
};

function RepoViewBranchList({
  rows,
  openTagDialog,
  openCreateDialog,
  openDeleteDialog,
  openMergeDialog,
  defaultReference,
  router,
  location,
  isDefault,
  isArcticSource,
  setReference: dispatchSetReference,
  noSearchResults,
}: RepoViewBranchTableProps & WithRouterProps) {
  const [catalogPrivileges] = useResourceSnapshot(
    ArcticCatalogPrivilegesResource || new SmartResource(() => null),
  );
  const showEmptyState = rows.length < 1;
  const { allRefsStatus: status, allRefsErr: err } =
    useContext(RepoViewContext);
  const { baseUrl, stateKey } = useNessieContext();
  const { isCatalog } = useVersionedPageContext();
  const id = useId();

  const renderRow = (cur: any, index: number): JSX.Element => {
    const id = cur.name + index;
    const goToDatasetOnClick = () => {
      dispatchSetReference({ reference: cur }, stateKey);

      router.push(
        constructVersionedEntityUrl({
          type: isCatalog ? "catalog" : "source",
          baseUrl,
          tab: "data",
          namespace: encodeURIComponent(cur.name),
        }),
      );
    };

    return (
      <div
        key={id}
        id={id}
        className="branch-list-item-container"
        onClick={
          isArcticSource
            ? () => router.push(`${location.pathname}/${cur.name}`)
            : goToDatasetOnClick
        }
        role="listitem"
        tabIndex={0}
        onKeyPress={(e) => {
          if (
            (e.code === "Space" || e.code === "Enter") &&
            e.target?.id === id
          ) {
            isArcticSource
              ? router.push(`${location.pathname}/${cur.name}`)
              : goToDatasetOnClick();
          }
        }}
        aria-label={`Branch ${cur.name}, link, go to data`}
      >
        <div data-testid={`brach-${cur.name}`} className="branch-list-item">
          <div className="branch-list-item-content">
            <span className="branch-list-item-name">
              <dremio-icon
                name="vcs/branch"
                class="branch-list-item-name-icon"
              />
              {cur.name}
            </span>

            {cur.metadata &&
              cur.metadata.commitMetaOfHEAD &&
              cur.metadata.commitMetaOfHEAD.authors?.[0] && (
                <div className="branch-list-item-content-bottom">
                  {cur.hash && cur.name && (
                    <span
                      className="branch-list-item-content-bottom-commit"
                      onClick={(e: React.SyntheticEvent) => stopPropagation(e)}
                    >
                      <dremio-icon name="vcs/commit" />
                      <CommitHash
                        branch={cur}
                        hash={cur.hash}
                        enableCopy={true}
                      />
                    </span>
                  )}
                  <span className="branch-list-item-divider"></span>
                  <span
                    onClick={(e) => stopPropagation(e)}
                    className="branch-list-item-message-container"
                  >
                    <Tooltip
                      title={
                        <span className="branch-list-item-message-tooltip">
                          {cur.metadata.commitMetaOfHEAD.message}
                        </span>
                      }
                    >
                      <span className="branch-list-item-message text-ellipsis">
                        {cur.metadata.commitMetaOfHEAD.message}
                      </span>
                    </Tooltip>
                  </span>
                  <span className="branch-list-item-by">by</span>
                  <Avatar
                    initials={nameToInitials(
                      cur.metadata.commitMetaOfHEAD.authors[0],
                    )}
                  />
                  <span className="branch-list-item-author text-ellipsis">
                    {cur.metadata.commitMetaOfHEAD.authors[0] || ""}
                  </span>
                  <span className="branch-list-item-divider"></span>
                  <span
                    onClick={(e) => stopPropagation(e)}
                    className="text-ellipsis"
                  >
                    {convertISOStringWithTooltip(
                      cur.metadata?.commitMetaOfHEAD?.authorTime?.toString?.() ??
                        "",
                      { isRelative: true },
                    )}
                  </span>
                </div>
              )}
          </div>
          <div
            className="branch-list-item-icons"
            onClick={(e: React.SyntheticEvent) => stopPropagation(e)}
          >
            {renderIcons(
              cur,
              isArcticSource,
              goToDatasetOnClick,
              openCreateDialog,
              openDeleteDialog,
              openMergeDialog,
              isDefault,
              openTagDialog,
              catalogPrivileges,
            )}
          </div>
        </div>
      </div>
    );
  };

  const renderCallToAction = (): JSX.Element =>
    noSearchResults ? (
      <EmptyStateContainer
        title="Common.NoResults"
        className="branch-list-no-results"
      />
    ) : (
      <EmptyStateContainer
        title="VersionedEntity.Branches.NoneYet"
        icon="vcs/create-branch"
      >
        <CatalogPrivilegeSwitch
          privilege={["branch", "canCreate"]}
          renderEnabled={() =>
            defaultReference && (
              <Button
                className="branch-list-empty-state-trigger"
                onClick={() => openCreateDialog(defaultReference)}
                variant="tertiary"
              >
                <FormattedMessage id="VersionedEntity.Branches.CreateBranch.EmptyState" />
              </Button>
            )
          }
        />
      </EmptyStateContainer>
    );

  return (
    <div className="branch-list">
      <div className="branch-list-name" id={id}>
        {isDefault ? (
          <FormattedMessage id="RepoView.DefaultBranch" />
        ) : (
          <FormattedMessage id="VersionedEntity.Branches.Other" />
        )}
      </div>
      <div className="branch-list-container" role="list" aria-labelledby={id}>
        <PromiseViewState status={status} error={err} />
        {showEmptyState ? renderCallToAction() : rows.map(renderRow)}
      </div>
    </div>
  );
}

const mapDispatchToProps = {
  setReference,
};

export default withRouter(
  connect(null, mapDispatchToProps)(RepoViewBranchList),
);
