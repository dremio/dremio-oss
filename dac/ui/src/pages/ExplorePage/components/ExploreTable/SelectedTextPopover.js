/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { Popover } from '@app/components/Popover';
import Immutable from 'immutable';
import { MAP, TEXT, LIST, MIXED, BINARY } from 'constants/DataTypes';
import Menu from 'components/Menus/Menu';
import MenuItemLink from 'components/Menus/MenuItemLink';
import MenuItem from 'components/Menus/MenuItem';
import Divider from '@material-ui/core/Divider';
import { withLocation } from 'containers/dremioLocation';

// todo: loc

@Radium
export class SelectedTextPopoverView extends Component {
  static propTypes = {
    hideDrop: PropTypes.func,
    copySelection: PropTypes.func,
    anchor: PropTypes.instanceOf(Element),
    columnName: PropTypes.string,
    columnType: PropTypes.string,
    location: PropTypes.object,
    visibleItems: PropTypes.array
  };

  constructor(props) {
    super(props);
    this.hideDrop = this.hideDrop.bind(this);
    this.items = Immutable.fromJS([
      {
        transform: 'extract',
        name: 'Extract…'
      },
      {
        transform: 'replace',
        name: 'Replace…'
      },
      {
        transform: 'split',
        name: 'Split…'
      },
      {
        transform: 'keeponly',
        name: 'Keep Only…'
      },
      {
        transform: 'exclude',
        name: 'Exclude…'
      }
    ]).filter((item) => {
      if (!props.visibleItems || !props.visibleItems.length) {
        return true;
      }
      return props.visibleItems.indexOf(item.get('transform')) !== -1;
    });

    this.state = {
      open: false
    };
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

    return type !== TEXT && type !== LIST && type !== MAP
      ? this.items.filter(item => item.get('transform') !== 'extract' && item.get('transform') !== 'split')
      : this.items;
  }

  hideDrop() {
    this.props.hideDrop();
    this.setState({ open: false });
  }

  renderForItemsOfList(newState) {
    const extract = this.items.filter(item => item.get('transform') === 'extract');
    return this.renderItem(extract.get(0), newState);
  }

  renderItem = (item, newState, index) => {
    const { location } = this.props;
    const href = {
      pathname: `${location.pathname}/details`,
      query: {
        ...location.query,
        type: 'transform'
      },
      state: {
        ...newState,
        transformType: item.get('transform')
      }
    };
    return (
      <MenuItemLink
        key={index}
        href={href}
        closeMenu={this.hideDrop}
        text={item.get('name')}
      />
    );
  }

  renderCopySelectionItem = () => {
    return (
      <MenuItem onClick={this.props.copySelection}>
        {la('Copy Selection')}
      </MenuItem>
    );
  }

  renderItems() {
    const { columnName, columnType, location} = this.props;
    const { state, query } = location;
    const type = (state || query).columnType || columnType;
    const items = this.getItemsForColumnType(type);
    const newState = {
      columnName,
      ...state,
      columnType: type,
      hasSelection: true
    };

    if (state && state.listOfItems && state.listOfItems.length > 1) {
      return (
        <Menu>
          {this.renderForItemsOfList(newState)}
          <Divider />
          {this.renderCopySelectionItem()}
        </Menu>
      );
    }
    return (
      <Menu>
        {items.map((item, index) => this.renderItem(item, newState, index))}
        {items.size && <Divider />}
        {this.renderCopySelectionItem()}
      </Menu>
    );
  }

  render() {
    return (
      <Popover
        anchorEl={this.state.open ? this.props.anchor : null}
        listRightAligned
        onClose={this.hideDrop}
        useLayerForClickAway
      >
        {this.renderItems()}
      </Popover>
    );
  }
}

export default withLocation(SelectedTextPopoverView);
