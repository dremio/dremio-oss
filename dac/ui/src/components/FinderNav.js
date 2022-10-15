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
import { Component } from "react";
import PropTypes from "prop-types";
import Immutable from "immutable";
import { withRouter } from "react-router";
import classNames from "classnames";
import { IconButton } from "dremio-ui-lib";
import FinderNavSection from "./FinderNavSection";
import LinkWithRef from "./LinkWithRef/LinkWithRef";
import { stopPropagation } from "@app/utils/reactEventUtils";

import "./FinderNav.less";

const MAX_TO_SHOW = Infinity;

//not pure because of active link in FinderNavItem
export class FinderNav extends Component {
  static propTypes = {
    title: PropTypes.string.isRequired,
    addTooltip: PropTypes.string,
    location: PropTypes.object,
    navItems: PropTypes.instanceOf(Immutable.List).isRequired,
    isInProgress: PropTypes.bool,
    isCollapsible: PropTypes.bool,
    isCollapsed: PropTypes.bool,
    onToggle: PropTypes.func,
    addHref: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    listHref: PropTypes.oneOfType([PropTypes.string, PropTypes.object]),
    children: PropTypes.node,
    renderExtra: PropTypes.func,
    noMarginTop: PropTypes.bool,
    router: PropTypes.any,
  };

  state = {
    collapsed: true,
  };

  onToggleClick = () => {
    const { onToggle } = this.props;
    if (onToggle) {
      onToggle();
    }
  };

  render() {
    const {
      title,
      addTooltip,
      navItems,
      isInProgress,
      addHref,
      listHref,
      children,
      isCollapsible,
      isCollapsed,
      noMarginTop,
      router,
    } = this.props;

    const wrapClass = classNames(
      "finder-nav",
      `${title.toLowerCase()}-wrap`,
      { "finder-nav--collapsible": isCollapsible },
      { "finder-nav--collapsed": isCollapsible && isCollapsed }
    ); // todo: don't use ui-string for code keys

    return (
      <div className={wrapClass}>
        <h4
          className={`finder-nav-title ${noMarginTop ? "no-margin-top" : ""} ${
            isCollapsible ? "no-padding-left" : ""
          }`}
          data-qa={title}
        >
          {listHref ? (
            <LinkWithRef
              className="pointer"
              activeClassName="active"
              to={listHref}
            >
              {isCollapsible ? (
                <>
                  <span
                    className="icon-container"
                    onClick={(e) => stopPropagation(e)}
                  >
                    <dremio-icon
                      name={
                        isCollapsed
                          ? "interface/right-chevron"
                          : "interface/down-chevron"
                      }
                      class="finder-nav__collapse-control"
                      onClick={this.onToggleClick}
                    />
                  </span>
                  <span>
                    {title} ({navItems.size})
                  </span>
                </>
              ) : (
                <span>
                  {title} ({navItems.size})
                </span>
              )}
              {addHref && (
                <IconButton
                  tooltip={addTooltip}
                  onClick={(e) => {
                    stopPropagation(e);
                    router.push(addHref);
                  }}
                  className="pull-right"
                  data-qa={`add-${title.toLowerCase()}`}
                >
                  <dremio-icon
                    name="interface/add-small"
                    class="add-space-icon"
                  />
                </IconButton>
              )}
            </LinkWithRef>
          ) : (
            `${title} (${navItems.size})`
          )}
        </h4>
        <div className="nav-list">
          {!isInProgress && (
            <FinderNavSection
              items={navItems}
              isInProgress={isInProgress}
              maxItemsCount={MAX_TO_SHOW}
              title={title}
              listHref={listHref}
              renderExtra={this.props.renderExtra}
            />
          )}
          {!isInProgress && children}
        </div>
      </div>
    );
  }
}

export default withRouter(FinderNav);
