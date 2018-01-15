/*
 * Copyright (C) 2017 Dremio Corporation
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
import React, { Component } from 'react';
import MenuMaterial from 'material-ui/Menu';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import DividerHr from './DividerHr';

@pureRender
export default class Menu extends Component {
  static propTypes = {
    onEscKeyDown: PropTypes.func,
    children: PropTypes.node,
    style: PropTypes.object,
    listStyle: PropTypes.object
  }

  getItems() {
    // ensure no duplicate or start/end dividers (e.g. due to filters)
    const items = [];
    React.Children.toArray(this.props.children).forEach((node) => {
      if (!(node.type === DividerHr)) {
        return items.push(node);
      }
      if (items.length !== 0 && !(items[items.length - 1].type === DividerHr)) {
        return items.push(node);
      }
    });
    if (items.length && items[items.length - 1].type === DividerHr) {
      items.pop();
    }

    return items;
  }

  render() {
    const { style, listStyle, onEscKeyDown } = this.props;

    return (
      <MenuMaterial
        data-qa='popover-menu'
        onEscKeyDown={onEscKeyDown}
        style={style ? style : styles.menuStyle}
        listStyle={listStyle ? listStyle : styles.listStyle}
        desktop>
        {this.getItems()}
      </MenuMaterial>
    );
  }
}

const styles = {
  menuStyle: {
    float: 'left',
    position: 'relative',
    zIndex: 1,
    padding: 0,
    overflow: 'hidden'
  },
  listStyle: {
    paddingTop: 5,
    paddingBottom: 5,
    width: 192
  }
};
