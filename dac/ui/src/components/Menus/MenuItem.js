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
import { Component, PropTypes } from 'react';
import Radium from 'radium';
import classNames from 'classnames';

import MenuItemMaterial from 'material-ui/MenuItem';
import { Popover } from 'material-ui/Popover';
import { body } from 'uiTheme/radium/typography';
import getMuiTheme from 'material-ui/styles/getMuiTheme';

import './MenuItem.less';

const CLOSE_SUBMENU_DELAY = 100;

@Radium
export default class MenuItem extends Component {
  static propTypes = {
    menuItems: PropTypes.array,
    rightIcon: PropTypes.object,
    onTouchTap: PropTypes.func,
    children: PropTypes.node,
    disabled: PropTypes.bool,
    isInformational: PropTypes.bool // shouldn't look intereactive
  };

  state = {
    open: false,
    anchorOrigin: {
      horizontal: 'right',
      vertical: 'top'
    },
    targetOrigin: {
      horizontal: 'left',
      vertical: 'top'
    }
  };

  delayedCloseTimer = null

  shouldClose(evt) {
    const enteredElement = evt.relatedTarget;

    if (enteredElement === window) {
      return true; // have seen this case
    }
    return (!this.refs.subMenu || !this.refs.subMenu.contains(enteredElement))
      && !this.refs.menuItem.contains(enteredElement);
  }

  handleMouseOver = () => {
    this.handleRequestOpen();
    clearTimeout(this.delayedCloseTimer);
  }

  handleMouseLeave = (evt) => {
    if (this.shouldClose(evt)) {
      this.delayedCloseTimer = setTimeout(this.handleRequestClose, CLOSE_SUBMENU_DELAY);
    }
  }

  handleRequestOpen = () => {
    this.setState({
      open: true
    });
  }

  handleRequestClose = () => {
    this.setState({
      open: false
    });
  }

  render() {
    const { menuItems, rightIcon, onTouchTap, disabled, isInformational } = this.props;
    const itemStyle = {...styles.menuItem, ...body, ...(isInformational && styles.informational)};
    const className = classNames({disabled}, 'menu-item-inner');
    return (
      <div>
        <MenuItemMaterial
          style={styles.resetStyle}
          innerDivStyle={styles.innerDivStyle}
          onTouchTap={onTouchTap}
          desktop>
          <div
            onMouseOver={this.handleMouseOver}
            onMouseLeave={this.handleMouseLeave}
            ref='menuItem'
            className={className}
            style={itemStyle}
          >
            {this.props.children}
            {rightIcon ? rightIcon : null}
          </div>
        </MenuItemMaterial>
        {
          // non-animated because if it is animated then if you quickly toggle #open
          // the popover will stay closed even when it should be open
          // (closing animation "wins" if it is in progress when open set to true?)
          // (plus it feels more responsive to not animate)
          //
          // and need to have extra `&& this.state.open` guard because otherwise can
          // throw an error trying to defocus sub-menu-items while tearing down on close
          //
          // but have to manually apply the theme zIndex because our package is a bit old
          // Can go away with DX-5368
          menuItems
            && this.state.open
            && <Popover
              style={{overflow: 'visible', zIndex: getMuiTheme().zIndex.popover}}
              useLayerForClickAway={false}
              open={this.state.open}
              anchorEl={this.refs.menuItem}
              anchorOrigin={this.state.anchorOrigin}
              targetOrigin={this.state.targetOrigin}
              onRequestClose={this.handleRequestClose}
              animated={false}>
              <div ref='subMenu' onMouseLeave={this.handleMouseLeave} onMouseOver={this.handleMouseOver}>
                {menuItems}
              </div>
            </Popover>
        }
      </div>
    );
  }
}

const styles = {
  resetStyle: {
    minHeight: 25,
    padding: 0,
    margin: 0
  },
  menuItem: {
    padding: '0 10px',
    height: 25,
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between'
  },
  innerDivStyle: {
    paddingLeft: 0,
    paddingRight: 0
  },
  informational: {
    backgroundColor: '#fff',
    cursor: 'default'
  }
};
