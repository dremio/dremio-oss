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

import FontIcon from "@app/components/Icon/FontIcon";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import NessieLink from "@app/pages/NessieHomePage/components/NessieLink/NessieLink";
import {
  constructArcticUrl,
  useArcticCatalogContext,
} from "@app/exports/pages/ArcticCatalog/arctic-catalog-utils";
import { withRouter, type WithRouterProps } from "react-router";
import { ArcticCatalogTabsType } from "../../ArcticCatalog";

import "./ArcticBreadcrumb.less";

type ArcticBreadcrumbProps = {
  path?: string[];
  maxItems?: number;
};

function getArcticLocationUrl(
  path: string[],
  i: number,
  tab: ArcticCatalogTabsType,
  branch: string,
  hash: string,
  type: "catalog" | "source"
) {
  return constructArcticUrl({
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
  arcticId?: string;
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

function ArcticBreadcrumb({
  path = [],
  maxItems = 3,
  params,
}: ArcticBreadcrumbProps & WithRouterProps) {
  const {
    source: { name },
    state: { hash },
  } = useNessieContext();
  const arcticCtx = useArcticCatalogContext();
  const popupState = usePopupState({
    variant: "popper",
    popupId: "nessieBreadcrumb-popper",
    disableAutoFocus: true,
  });
  const toggleProps = bindToggle(popupState);

  const pathLinks = useMemo(() => path.slice(0, -1), [path]);
  const lastItem =
    path.length > 0 ? <Item text={path[path.length - 1]} /> : null;

  const arcticId = params?.arcticCatalogId;
  const arcticTab = arcticCtx.activeTab;
  const branchName = params?.branchName;
  const hashParam = hash ? `?hash=${hash}` : "";
  const type = arcticCtx?.isCatalog ? "catalog" : "source";

  return (
    <div className="nessieBreadcrumb arcticBreadcrumb">
      <Breadcrumbs separator=".">
        <Item
          {...(path.length
            ? {
                to: constructArcticUrl({
                  type: type,
                  baseUrl: "",
                  tab: arcticTab,
                  namespace: encodeURIComponent(branchName),
                  hash: hashParam,
                }),
              }
            : {})}
          text={name}
          arcticId={arcticId}
        />
        {path.length > maxItems && (
          <span className="nessieBreadcrumb-menuTrigger">
            <FontIcon type="MoreIcon" {...toggleProps} />
          </span>
        )}
        {path.length <= maxItems &&
          pathLinks.map((cur, i) => (
            <Item
              key={i}
              to={getArcticLocationUrl(
                path,
                i,
                arcticTab,
                encodeURIComponent(branchName),
                hashParam,
                type
              )}
              text={decodeURIComponent(cur)}
              arcticId={arcticId}
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
                        to={getArcticLocationUrl(
                          path,
                          i,
                          arcticTab,
                          encodeURIComponent(branchName),
                          hashParam,
                          type
                        )}
                        className="nessieBreadcrumb-menuItem"
                      >
                        <FontIcon type="FolderNessie" />
                        <span className="nessieBreadcrumb-menuItemContent">
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
export default withRouter(ArcticBreadcrumb);
