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
import { Popover } from "@app/components/Popover";
import Immutable from "immutable";
import {
  MAP,
  TEXT,
  LIST,
  MIXED,
  BINARY,
  STRUCT,
} from "@app/constants/DataTypes";
import Menu from "components/Menus/Menu";
import MenuItemLink from "components/Menus/MenuItemLink";
import MenuItem from "components/Menus/MenuItem";
import Divider from "@mui/material/Divider";
import { withLocation } from "containers/dremioLocation";
import { getSupportFlag } from "@app/exports/endpoints/SupportFlags/getSupportFlag";
import { ALLOW_DOWNLOAD } from "@app/exports/endpoints/SupportFlags/supportFlagConstants";

import "./SelectedTextPopover.less";

// todo: loc

export class SelectedTextPopoverView extends Component {
  static propTypes = {
    hideDrop: PropTypes.func,
    copySelection: PropTypes.func,
    anchor: PropTypes.instanceOf(Element),
    columnName: PropTypes.string,
    columnType: PropTypes.string,
    location: PropTypes.object,
    visibleItems: PropTypes.array,
  };

  constructor(props) {
    super(props);
    this.hideDrop = this.hideDrop.bind(this);
    this.items = Immutable.fromJS([
      {
        transform: "extract",
        name: "Extract…",
      },
      {
        transform: "replace",
        name: "Replace…",
      },
      {
        transform: "split",
        name: "Split…",
      },
      {
        transform: "keeponly",
        name: "Keep Only…",
      },
      {
        transform: "exclude",
        name: "Exclude…",
      },
    ]).filter((item) => {
      if (!props.visibleItems || !props.visibleItems.length) {
        return true;
      }
      return props.visibleItems.indexOf(item.get("transform")) !== -1;
    });

    this.state = {
      open: false,
      allowDownload: true,
    };
  }

  async componentDidMount() {
    try {
      const res = await getSupportFlag(ALLOW_DOWNLOAD);
      this.setState({
        allowDownload: res?.value,
      });
    } catch (e) {
      //
    }
  }

  componentWillMount() {
    if (this.props.anchor) {
      this.setState({ open: true });
    }
  }

  componentWillReceiveProps(nextProps) {
    if (this.props.anchor !== nextProps.anchor && nextProps.anchor) {
      this.setState({ open: true });
    }
  }

  getItemsForColumnType = (type) => {
    if (type === MIXED || type === BINARY) {
      return Immutable.List();
    }

    return type !== TEXT && type !== LIST && type !== MAP && type !== STRUCT
      ? this.items.filter(
          (item) =>
            item.get("transform") !== "extract" &&
            item.get("transform") !== "split"
        )
      : this.items;
  };

  hideDrop() {
    this.props.hideDrop();
    this.setState({ open: false });
  }

  renderForItemsOfList(newState) {
    const extract = this.items.filter(
      (item) => item.get("transform") === "extract"
    );
    if (extract && extract.size > 0) {
      return this.renderItem(extract.get(0), newState);
    }
    return null;
  }

  renderItem = (item, newState, index) => {
    const { location } = this.props;
    const href = {
      // This prevents having more than one '/details' in the URL in  case the user does a chain of SQL transformations.
      // Having more than one '/details' in the URL will cause the 404 page to render since it's an invalid path.
      pathname: `${location.pathname}${
        !location.pathname.endsWith("/details") ? "/details" : ""
      }`,
      query: {
        ...location.query,
        type: "transform",
      },
      state: {
        ...newState,
        transformType: item.get("transform"),
      },
    };
    return (
      <MenuItemLink
        key={index}
        href={href}
        closeMenu={this.hideDrop}
        text={item.get("name")}
      />
    );
  };

  renderCopySelectionItem = () => {
    return (
      <MenuItem onClick={this.props.copySelection}>
        {la("Copy Selection")}
      </MenuItem>
    );
  };

  renderItems() {
    const { columnName, columnType, location } = this.props;
    const { state, query } = location;
    const type = (state || query).columnType || columnType;
    const items = this.getItemsForColumnType(type);
    const newState = {
      columnName,
      ...state,
      columnType: type,
      hasSelection: true,
    };

    if (state && state.listOfItems && state.listOfItems.length > 1) {
      const itemsList = this.renderForItemsOfList(newState);
      return (
        <Menu>
          {itemsList}
          {itemsList && this.state.allowDownload && <Divider />}
          {this.state.allowDownload && this.renderCopySelectionItem()}
        </Menu>
      );
    }
    return (
      <Menu>
        {items.map((item, index) => this.renderItem(item, newState, index))}
        {items.size > 0 && this.state.allowDownload && <Divider />}
        {this.state.allowDownload && this.renderCopySelectionItem()}
      </Menu>
    );
  }

  render() {
    const {
      location: { state: locationState },
    } = this.props;

    if (!(locationState?.listOfItems?.length > 1 || this.state.allowDownload)) {
      return null;
    }
    return (
      <Popover
        anchorEl={this.state.open ? this.props.anchor : null}
        listRightAligned
        onClose={this.hideDrop}
        useLayerForClickAway
        classes={{
          root: "selectedTextPopover__root",
        }}
      >
        {this.renderItems()}
      </Popover>
    );
  }
}
export default withLocation(SelectedTextPopoverView);
