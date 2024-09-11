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
import { useMemo } from "react";
import { Breadcrumbs, ClickAwayListener, Fade, Popper } from "@mui/material";
import { bindToggle, bindPopper } from "material-ui-popup-state";
import { usePopupState } from "material-ui-popup-state/hooks";

import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import {
  constructVersionedEntityUrl,
  useVersionedPageContext,
} from "@app/exports/pages/VersionedHomePage/versioned-page-utils";
import { withRouter, type WithRouterProps } from "react-router";
import { VersionedPageTabsType } from "@app/exports/pages/VersionedHomePage/VersionedHomePage";
import { IconButton } from "dremio-ui-lib/components";

import "./VersionedPageBreadcrumb.less";

type VersionedBreadcrumbProps = {
  path?: string[];
  maxItems?: number;
};

function getVersionedLocationUrl(
  path: string[],
  i: number,
  tab: VersionedPageTabsType,
  branch: string,
  hash: string,
  type: "catalog" | "source",
) {
  return constructVersionedEntityUrl({
    type,
    baseUrl: "",
    tab,
    namespace: `${branch}/${path.slice(0, i + 1).join("/")}`,
    hash,
  });
}

function Item({
  text: encodedText,
  to,
  onClick,
}: {
  text: string;
  to?: string | null;
  versionedEntityId?: string;
  onClick?: (arg: any) => void;
}) {
  const text = decodeURIComponent(encodedText);
  const props = {
    title: text,
    className: "nessieBreadcrumb-item text-ellipsis",
    onClick,
  };

  if (to) {
    return (
      <NessieLink to={to} {...props}>
        {text}
      </NessieLink>
    );
  } else {
    return <span {...props}>{text}</span>;
  }
}

function VersionedBreadcrumb({
  path = [],
  maxItems = 3,
  params,
}: VersionedBreadcrumbProps & WithRouterProps) {
  const {
    source: { name },
    state: { hash },
  } = useNessieContext();
  const versionedCtx = useVersionedPageContext();
  const popupState = usePopupState({
    variant: "popper",
    popupId: "nessieBreadcrumb-popper",
    disableAutoFocus: true,
  });
  const toggleProps = bindToggle(popupState);

  const pathLinks = useMemo(() => path.slice(0, -1), [path]);
  const lastItem =
    path.length > 0 ? <Item text={path[path.length - 1]} /> : null;

  const versionedEntityId = params?.versionedEntityId;
  const versionedTab = versionedCtx.activeTab;
  const branchName = params?.branchName;
  const hashParam = hash ? `?hash=${hash}` : "";
  const type = versionedCtx?.isCatalog ? "catalog" : "source";

  return (
    <div className="nessieBreadcrumb versionedBreadcrumb">
      <Breadcrumbs separator=".">
        <Item
          {...(path.length
            ? {
                to: constructVersionedEntityUrl({
                  type: type,
                  baseUrl: "",
                  tab: versionedTab,
                  namespace: encodeURIComponent(branchName),
                  hash: hashParam,
                }),
              }
            : {})}
          text={name}
          versionedEntityId={versionedEntityId}
        />
        {path.length > maxItems && (
          <span className="nessieBreadcrumb-menuTrigger">
            <IconButton {...toggleProps} aria-label="More...">
              <dremio-icon name="common/Ellipsis"></dremio-icon>
            </IconButton>
          </span>
        )}
        {path.length <= maxItems &&
          pathLinks.map((cur, i) => (
            <Item
              key={i}
              to={getVersionedLocationUrl(
                path,
                i,
                versionedTab,
                encodeURIComponent(branchName),
                hashParam,
                type,
              )}
              text={decodeURIComponent(cur)}
              versionedEntityId={versionedEntityId}
            />
          ))}
        {lastItem}
      </Breadcrumbs>
      {path.length > maxItems && (
        <Popper {...bindPopper(popupState)} placement="bottom-start" transition>
          {({ TransitionProps }) => (
            <ClickAwayListener onClickAway={popupState.close}>
              <Fade {...TransitionProps} timeout={250}>
                <div className="menuContent">
                  {pathLinks.map((cur, i) => (
                    <div
                      key={i}
                      className="menuContent-item"
                      onClick={popupState.close}
                    >
                      <NessieLink
                        to={getVersionedLocationUrl(
                          path,
                          i,
                          versionedTab,
                          encodeURIComponent(branchName),
                          hashParam,
                          type,
                        )}
                        className="nessieBreadcrumb-menuItem flex items-center"
                      >
                        <dremio-icon name="entities/blue-folder"></dremio-icon>
                        <span className="nessieBreadcrumb-menuItemContent ml-05">
                          {decodeURIComponent(cur)}
                        </span>
                      </NessieLink>
                    </div>
                  ))}
                </div>
              </Fade>
            </ClickAwayListener>
          )}
        </Popper>
      )}
    </div>
  );
}
export default withRouter(VersionedBreadcrumb);
