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
import { Component, createRef, PureComponent } from "react";
import { connect } from "react-redux";
import classNames from "clsx";
import Immutable from "immutable";
import { Tooltip } from "dremio-ui-lib";
import { withRouter } from "react-router";

import PropTypes from "prop-types";

import EntityLink from "@app/pages/HomePage/components/EntityLink";
import { EntityIcon } from "@app/pages/HomePage/components/EntityIcon";
import { EntityName } from "@app/pages/HomePage/components/EntityName";
import { Popover, MouseEvents } from "@app/components/Popover";

import AllSpacesMenu from "components/Menus/HomePage/AllSpacesMenu";
import AllSourcesMenu from "components/Menus/HomePage/AllSourcesMenu";
import { ENTITY_TYPES } from "@app/constants/Constants";
import { getRootEntityTypeByIdV3 } from "@app/selectors/home";
import ContainerDatasetCountV3, {
  ContainerDatasetCount,
} from "@app/pages/HomePage/components/ContainerDatasetCount";

import ResourcePin from "./ResourcePin";
import EllipsedText from "./EllipsedText";

import "./FinderNavItem.less";

const mapStateToPropsV3 = (state, { entityId }) => {
  const type = getRootEntityTypeByIdV3(state, entityId);
  const props = {
    entityType: type,
  };

  return props;
};

// a component that would be used for V3 api responses
@connect(mapStateToPropsV3)
export class FinderNavItemV3 extends PureComponent {
  static propTypes = {
    //public api
    entityId: PropTypes.string.isRequired,
    // connected
    entityType: PropTypes.oneOf([
      ENTITY_TYPES.home,
      ENTITY_TYPES.source,
      ENTITY_TYPES.space,
    ]).isRequired,
  };

  render() {
    const { entityId } = this.props;

    return (
      <EntityLink
        entityId={entityId}
        activeClassName="active"
        className="finder-nav-item-link"
      >
        <EntityIcon entityId={entityId} />
        <EntityName
          entityId={entityId}
          style={{ marginRight: 5, width: "191px" }}
        />
        <ContainerDatasetCountV3 entityId={entityId} />
        {entityId && <ResourcePin entityId={entityId} />}
      </EntityLink>
    );
  }
}

const mapStateToProps = (state, { item }) => ({
  entityType: getRootEntityTypeByIdV3(state, item.id),
});

class FinderNavItem extends Component {
  static propTypes = {
    item: PropTypes.object.isRequired,
    style: PropTypes.object,
    noHover: PropTypes.bool,
    isHomeActive: PropTypes.bool,

    //connected
    entityType: PropTypes.oneOf([
      ENTITY_TYPES.home,
      ENTITY_TYPES.source,
      ENTITY_TYPES.space,
    ]).isRequired,
    renderExtra: PropTypes.any,
    params: PropTypes.object,
  };

  constructor(props) {
    super(props);
    this.state = {
      menuOpen: false,
    };
    this.lastMouseEventPosition = null;
  }

  handleRightClick = (e) => {
    // home space does not have context menu
    if (!this.hasMenu(this.props.entityType)) return;

    e.preventDefault();
    this.lastMouseEventPosition = this.rightClickPosition(e);
    this.setState({
      menuOpen: true,
      anchorEl: e.currentTarget,
    });
  };

  hasMenu = (entityType) => entityType !== ENTITY_TYPES.home;

  // make position string for comparing mouse events
  rightClickPosition = (e) => `x: ${e.clientX}, y: ${e.clientY}`;
  clickAwayPosition = (e) => `x: ${e.x}, y: ${e.y}`;

  handleMenuClose = () => {
    this.setState({ menuOpen: false });
  };

  getMenu = () => {
    const { item } = this.props;
    switch (item.entityType) {
      case ENTITY_TYPES.space:
        return (
          <AllSpacesMenu spaceId={item.id} closeMenu={this.handleMenuClose} />
        );
      case ENTITY_TYPES.source:
        return (
          <AllSourcesMenu
            item={Immutable.fromJS(item)}
            closeMenu={this.handleMenuClose}
          />
        );
      default:
        return null;
    }
  };

  itemRef = createRef(null);
  render() {
    const { style, entityType, renderExtra, isHomeActive, params } = this.props;

    const { id, name, numberOfDatasets, disabled, datasetCountBounded } =
      this.props.item;

    const itemClass = classNames("finder-nav-item", {
      withExtra: !!renderExtra,
    });
    const isActiceArcticSource = name === params?.sourceId; // sourceId is param for when in Arctic Source history URL

    return (
      <li
        className={itemClass}
        style={{ ...(disabled && styles.disabled), ...(style || {}) }}
        ref={this.itemRef}
      >
        {entityType === ENTITY_TYPES.space ? (
          <div
            className="full-width full-height"
            onContextMenu={this.handleRightClick}
          >
            <FinderNavItemV3 entityId={id} />
          </div>
        ) : (
          <div
            className="entity-link-wrapper full-height"
            onContextMenu={this.handleRightClick}
          >
            <EntityLink
              entityId={id}
              activeClassName="active"
              className={`finder-nav-item-link ${
                isHomeActive || isActiceArcticSource ? "active" : ""
              } `}
            >
              <EntityIcon entityId={id} />
              <Tooltip title={name}>
                <EllipsedText className="nav-item-ellipsed-text">
                  <span>{name}</span>
                </EllipsedText>
              </Tooltip>
              {renderExtra && (
                <span className="extra-content">
                  {renderExtra(this.props.item, this.itemRef)}
                </span>
              )}
              {!renderExtra && (
                <>
                  <ContainerDatasetCount
                    count={numberOfDatasets}
                    isBounded={datasetCountBounded}
                  />
                  {id && <ResourcePin entityId={id} />}
                </>
              )}
            </EntityLink>
          </div>
        )}
        {this.hasMenu(this.props.entityType) && this.state.menuOpen && (
          <Popover
            useLayerForClickAway={false}
            anchorEl={this.state.menuOpen ? this.state.anchorEl : null}
            listRightAligned
            onClose={this.handleMenuClose}
            clickAwayMouseEvent={MouseEvents.onMouseDown}
          >
            {this.getMenu()}
          </Popover>
        )}
      </li>
    );
  }
}

export default withRouter(connect(mapStateToProps)(FinderNavItem));

const styles = {
  disabled: {
    opacity: 0.7,
    background: "#fff",
    pointerEvents: "none",
    color: "#999",
  },
};
