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
import {
  Breadcrumbs,
  ClickAwayListener,
  Fade,
  Popper,
} from "@material-ui/core";
import { bindToggle, bindPopper } from "material-ui-popup-state";
import { usePopupState } from "material-ui-popup-state/hooks";

import FontIcon from "@app/components/Icon/FontIcon";
import { useNessieContext } from "../../utils/context";
import NessieLink from "../NessieLink/NessieLink";

import "./NessieBreadcrumb.less";

type NessieBreadcrumbProps = {
  path?: string[];
  maxItems?: number;
};

function getNamespaceUrl(path: string[], i: number) {
  return `/namespace/${path.slice(0, i + 1).join("/")}`;
}

function Item({
  text: encodedText,
  to,
  onClick,
}: {
  text: string;
  to?: string | null;
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

function NessieBreadcrumb({ path = [], maxItems = 3 }: NessieBreadcrumbProps) {
  const {
    source: { name },
  } = useNessieContext();
  const popupState = usePopupState({
    variant: "popper",
    popupId: "nessieBreadcrumb-popper",
    disableAutoFocus: true,
  });
  const toggleProps = bindToggle(popupState);

  const pathLinks = useMemo(() => path.slice(0, -1), [path]);
  const lastItem =
    path.length > 0 ? <Item text={path[path.length - 1]} /> : null;

  const iconStyle = { width: "24px", height: "24px" };

  return (
    <div className="nessieBreadcrumb">
      <span className="nessieBreadcrumb-repoIcon">
        <FontIcon type="Repository" theme={{ Icon: iconStyle }} />
      </span>
      <Breadcrumbs separator=".">
        <Item to="/" text={name} />
        {path.length > maxItems && (
          <span className="nessieBreadcrumb-menuTrigger">
            <FontIcon type="MoreIcon" {...toggleProps} />
          </span>
        )}
        {path.length <= maxItems &&
          pathLinks.map((cur, i) => (
            <Item
              key={i}
              to={getNamespaceUrl(path, i)}
              text={decodeURIComponent(cur)}
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
                        to={getNamespaceUrl(path, i)}
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
export default NessieBreadcrumb;
