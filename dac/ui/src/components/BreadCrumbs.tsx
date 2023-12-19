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
import { useState, useRef, ReactNode } from "react";
//@ts-ignore
import { Link } from "react-router";
import Immutable from "immutable";
//@ts-ignore
import invariant from "invariant";
import Menu from "@mui/material/Menu";
import MenuItem from "@mui/material/MenuItem";
import { CopyButton } from "dremio-ui-lib/components";
//@ts-ignore
import { IconButton, Tooltip } from "dremio-ui-lib";
import { splitFullPath } from "utils/pathUtils";

import {
  rmProjectBase,
  addProjectBase,
} from "dremio-ui-common/utilities/projectBase.js";

import "./BreadCrumbs.less";

type BreadCrumbsTypes = {
  fullPath: Immutable.List<any>;
  pathname: string;
  linkStyle?: React.CSSProperties;
  longCrumbs: boolean;
  hideLastItem?: boolean;
  showCopyButton?: boolean;
  includeQuotes?: boolean;
  extraContent?: ReactNode;
};

export function formatFullPath(fullPath: any) {
  invariant(fullPath && fullPath.map, "fullPath should be an Immutable.List");
  // if fullPath only contains one item, don't quote if it has a . in its name
  return fullPath.map((item: string | string[]) =>
    fullPath.size > 1 && item.includes(".") ? `"${item}"` : item
  );
}

type BreadCrumbItemProps = {
  longCrumbs: boolean;
  children: any;
};

const BreadCrumbItem = ({ longCrumbs, children }: BreadCrumbItemProps) => {
  const ref = useRef<any>(null);
  const title =
    (children.key && children.key.substring(0, children.key.length - 1)) ||
    "Link";
  const isEllipised =
    ref.current && ref.current.offsetWidth < ref.current.scrollWidth;
  const addMinWidth = title.length < 15 ? " addMinWidth" : "";
  const longCrumb = longCrumbs ? "long-crumb" : "crumb";
  const classes = `${longCrumb} ${addMinWidth}`;
  return isEllipised ? (
    <Tooltip title={title}>
      <div ref={ref} className={classes}>
        {children}
      </div>
    </Tooltip>
  ) : (
    <div ref={ref} className={classes}>
      {children}
    </div>
  );
};

const BreadCrumbs = ({
  longCrumbs = true,
  fullPath,
  pathname,
  hideLastItem = false,
  linkStyle,
  showCopyButton = false,
  includeQuotes = false,
  extraContent,
}: BreadCrumbsTypes) => {
  const [anchorEl, setAnchorEl] = useState(null);
  const formattedFullPath = formatFullPath(fullPath);
  const els = getPathElements(formattedFullPath, pathname, linkStyle);

  const renderPath = () => {
    // if there are more than 3 then breadcrumbs will use a menu
    const crumbs = hideLastItem ? [...els.slice(0, -1)] : [...els];
    if (crumbs.length > 3) {
      const firstCrumb = crumbs[0];
      const lastCrumb = crumbs[crumbs.length - 1];
      const secondToLastCrumb = crumbs[crumbs.length - 2];
      crumbs.pop();
      crumbs.pop();
      crumbs.shift();
      return (
        <>
          <BreadCrumbItem longCrumbs={longCrumbs}>{firstCrumb}</BreadCrumbItem>
          {longCrumbs ? (
            <div className="icon-container">
              <span className="spacing">.</span>
              <IconButton
                aria-label="Nested folders"
                onClick={handleOpen}
                className="icon-container-breadcrumb-more"
              >
                <dremio-icon name="interface/more" />
              </IconButton>
              <span className="spacing">.</span>
            </div>
          ) : (
            <div>
              {" "}
              .<a onClick={handleOpen}> ... </a>.
            </div>
          )}
          <BreadCrumbItem longCrumbs={longCrumbs}>
            {secondToLastCrumb}
          </BreadCrumbItem>
          <span className={longCrumbs ? "spacing" : ""}>.</span>
          <BreadCrumbItem longCrumbs={longCrumbs}>{lastCrumb}</BreadCrumbItem>
          {fullPath && showCopyButton && (
            <CopyButton
              className="copy-button"
              contents={
                includeQuotes
                  ? formatFullPath(fullPath).join(".")
                  : fullPath.join(".")
              }
              size="L"
            />
          )}
          <Menu
            disableAutoFocusItem
            elevation={0}
            anchorOrigin={{
              vertical: "bottom",
              horizontal: "right",
            }}
            transformOrigin={{
              vertical: "top",
              horizontal: "left",
            }}
            classes={{ paper: "paper" }}
            id="crumb-menu"
            anchorEl={anchorEl}
            open={Boolean(anchorEl)}
            onClose={handleClose}
          >
            {crumbs.map((item) => (
              <MenuItem key={item.key}>
                <div title={item.props.children} className="menu-crumb">
                  {item}
                </div>
              </MenuItem>
            ))}
          </Menu>
        </>
      );
    }

    //@ts-ignore
    return (
      <>
        {(hideLastItem ? els.slice(0, -1) : els).map(
          (el: any, index: any, arr: any[]) => {
            return index < arr.length - 1 ? (
              <>
                <BreadCrumbItem longCrumbs={longCrumbs} key={index}>
                  {el}
                </BreadCrumbItem>
                <span>.</span>
              </>
            ) : (
              <BreadCrumbItem longCrumbs={longCrumbs} key={index}>
                {el}
              </BreadCrumbItem>
            );
          },
          []
        )}
        {extraContent && <div className="margin-left">{extraContent}</div>}
        {fullPath && showCopyButton && (
          <CopyButton
            className="copy-button"
            contents={
              includeQuotes
                ? formatFullPath(fullPath).join(".")
                : fullPath.join(".")
            }
            size="L"
          />
        )}
      </>
    );
  };
  // opens the menu
  const handleOpen = (event: any) => {
    setAnchorEl(event.currentTarget);
  };

  // closes the menu
  const handleClose = () => {
    setAnchorEl(null);
  };

  const containerClasses = longCrumbs
    ? "Breadcrumbs"
    : "Breadcrumbs shortMaxWidth";
  return (
    <div
      onMouseOver={(e) => e.stopPropagation()}
      onFocus={(e) => e.stopPropagation()}
      className={containerClasses}
    >
      {renderPath()}{" "}
    </div>
  );
};

export function getPathElements(
  fullPath: any,
  pathname: string,
  linkStyle: any | undefined
) {
  const lastItem = fullPath.last();
  const fullPathWithoutLastItem = fullPath.slice(0, -1);
  const path = fullPathWithoutLastItem
    .reduce((prev: any, cur: any) => {
      return prev.concat(splitFullPath(cur));
    }, Immutable.List())
    .concat(lastItem);

  return path
    .map((item: any, index: number) => {
      if (index === path.count() - 1) {
        return (
          <span key={item + index} style={linkStyle}>
            {item}
          </span>
        );
      }

      const href = getPartialPath(index, path, pathname);

      return (
        <Link key={item + index} to={href} style={linkStyle}>
          {item}
        </Link>
      );
    })
    .toJS();
}

// TODO Use ui-common paths
export function getPartialPath(index: number, fullPath: any, pathname: string) {
  // NOTE: This would be a lot easier if we had the parent container's url.
  // Instead we need to get the first part of the path (/space or /source) from the current location.pathname
  const pathnameParts = (rmProjectBase(pathname) || "/").split("/");
  const fullPathRoot = fullPath.get(index);
  let partialPath;
  if (index === 0) {
    if (fullPathRoot[0] === "@") {
      partialPath = "/";
    } else {
      partialPath = `/${pathnameParts[1]}/${encodeURIComponent(fullPathRoot)}`;
    }
  } else {
    const encodedFullPath = fullPath.map((part: string) =>
      encodeURIComponent(part)
    );
    partialPath = `/${pathnameParts[1]}/${encodedFullPath.get(
      0
    )}/folder/${encodedFullPath.slice(1, index + 1).join("/")}`;
  }

  return addProjectBase(partialPath);
}

export default BreadCrumbs;
