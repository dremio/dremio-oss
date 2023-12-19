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

import { useContext, useEffect, useState } from "react";
import { FormattedMessage } from "react-intl";
import { withRouter, WithRouterProps } from "react-router";
import { connect } from "react-redux";

import { AutoSizer, List } from "react-virtualized";
import { MenuItem } from "@mui/material";
import CommitHash from "@app/pages/HomePage/components/BranchPicker/components/CommitBrowser/components/CommitHash/CommitHash";
//@ts-ignore
import { Tooltip } from "dremio-ui-lib";
import { Avatar } from "dremio-ui-lib/components";
import PromiseViewState from "@app/components/PromiseViewState/PromiseViewState";
import { Reference } from "@app/types/nessie";
import { RepoViewContext } from "../../../../RepoView";
import { convertISOStringWithTooltip, renderIcons } from "./utils";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import { nameToInitials } from "@app/exports/utilities/nameToInitials";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { setReference } from "@app/actions/nessie/nessie";
import { stopPropagation } from "@app/utils/reactEventUtils";
import EmptyStateContainer from "@app/pages/HomePage/components/EmptyStateContainer";
import { useResourceSnapshot } from "smart-resource/react";
import { ArcticCatalogPrivilegesResource } from "@inject/arctic/resources/ArcticCatalogPrivilegesResource";
import { CatalogPrivilegeSwitch } from "@app/exports/components/CatalogPrivilegeSwitch/CatalogPrivilegeSwitch";

import "./RepoViewBranchList.less";
import { SmartResource } from "smart-resource";

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
    ArcticCatalogPrivilegesResource || new SmartResource(() => null)
  );
  const showEmptyState = rows.length < 1;
  const { allRefsStatus: status, allRefsErr: err } =
    useContext(RepoViewContext);
  const [rowHover, setRowHover] = useState<boolean[]>(
    new Array(rows.length).fill(false)
  );

  const { baseUrl, stateKey } = useNessieContext();

  const { isCatalog } = useArcticCatalogContext();

  useEffect(() => {
    setRowHover(new Array(rows.length).fill(false));
  }, [rows.length]);

  const renderRow = ({ index, key, style }: any): JSX.Element => {
    const cur = rows[index];

    const goToDatasetOnClick = () => {
      dispatchSetReference({ reference: cur }, stateKey);

      router.push(
        constructArcticUrl({
          type: isCatalog ? "catalog" : "source",
          baseUrl,
          tab: "data",
          namespace: encodeURIComponent(cur.name),
        })
      );
    };

    return (
      <div
        key={key}
        style={style}
        className="branch-list-item-container"
        onMouseEnter={() => {
          const copyHover = new Array(rowHover.length).fill(false);
          copyHover[key.split("-")[0]] = true;
          setRowHover(copyHover);
        }}
        onMouseLeave={() => setRowHover(new Array(rowHover.length).fill(false))}
        onClick={
          isArcticSource
            ? () => router.push(`${location.pathname}/${cur.name}`)
            : goToDatasetOnClick
        }
      >
        <MenuItem
          data-testid={`brach-${cur.name}`}
          className="branch-list-item"
        >
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
                      <span className="branch-list-item-message">
                        {cur.metadata.commitMetaOfHEAD.message}
                      </span>
                    </Tooltip>
                  </span>
                  <span className="branch-list-item-by">by</span>
                  <Avatar
                    initials={nameToInitials(
                      cur.metadata.commitMetaOfHEAD.authors[0]
                    )}
                  />
                  <span className="branch-list-item-author">
                    {cur.metadata.commitMetaOfHEAD.authors[0] || ""}
                  </span>
                  <span className="branch-list-item-divider"></span>
                  <span onClick={(e) => stopPropagation(e)}>
                    {convertISOStringWithTooltip(
                      cur.metadata?.commitMetaOfHEAD?.authorTime?.toString?.() ??
                        "",
                      { isRelative: true }
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
              rowHover[key.split("-")[0]],
              isArcticSource,
              goToDatasetOnClick,
              openCreateDialog,
              openDeleteDialog,
              openMergeDialog,
              isDefault,
              openTagDialog,
              catalogPrivileges
            )}
          </div>
        </MenuItem>
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
        title="ArcticCatalog.Branches.NoneYet"
        icon="vcs/create-branch"
      >
        <CatalogPrivilegeSwitch
          privilege={["branch", "canCreate"]}
          renderEnabled={() =>
            defaultReference && (
              <span
                className="branch-list-empty-state-trigger"
                onClick={() => openCreateDialog(defaultReference)}
              >
                <FormattedMessage id="ArcticCatalog.Branches.CreateBranch.EmptyState" />
              </span>
            )
          }
        />
      </EmptyStateContainer>
    );

  return (
    <div className="branch-list">
      <div className="branch-list-name">
        {isDefault ? (
          <FormattedMessage id="RepoView.DefaultBranch" />
        ) : (
          <FormattedMessage id="ArcticCatalog.Branches.Other" />
        )}
      </div>
      <div className="branch-list-container">
        <PromiseViewState status={status} error={err} />
        <AutoSizer>
          {({ height }) => (
            <List
              rowRenderer={showEmptyState ? renderCallToAction : renderRow}
              rowCount={showEmptyState ? 1 : rows.length}
              rowHeight={showEmptyState && !noSearchResults ? height : 82}
              height={height}
              width={1}
            />
          )}
        </AutoSizer>
      </div>
    </div>
  );
}

const mapDispatchToProps = {
  setReference,
};

export default withRouter(
  connect(null, mapDispatchToProps)(RepoViewBranchList)
);
