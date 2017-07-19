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
import React, { Component, PropTypes } from 'react';
import Radium from 'radium';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import { triangleTop } from 'uiTheme/radium/overlay';

@Radium
export default class HeaderDropdown extends Component {
  static propTypes = {
    dataQa: PropTypes.string,
    name: PropTypes.string.isRequired,
    menu: PropTypes.node.isRequired
  };

  static defaultProps = {
    name: ''
  };

  state = {
    open: false
  }

  handleClick = (event) => {
    this.setState({
      open: true,
      anchorEl: event.currentTarget
    });
  }

  handleRequestClose = () => {
    this.setState({open: false});
  }

  render() {
    return (
      <div data-qa={this.props.dataQa}>
        <div className='item' onClick={this.handleClick} style={styles.navItem}>
          <span className='item-wrap' style={styles.name}>{this.props.name}</span>
          <i className='fa fa-angle-down' style={styles.downArrow}/>
        </div>
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{horizontal: 'right', vertical: 'bottom'}}
          targetOrigin={{horizontal: 'right', vertical: 'top'}}
          onRequestClose={this.handleRequestClose}
          animation={PopoverAnimationVertical}
          style={styles.popover}
        >
          <div style={styles.triangle}/>
          {React.cloneElement(this.props.menu, {closeMenu: this.handleRequestClose})}
        </Popover>
      </div>
    );
  }
}

const styles = {
  avatar: {
    marginRight: 5
  },
  navItem: {
    height: '100%'
  },
  name: {
    margin: '0 6px 0 0'
  },
  downArrow: {
    fontSize: '18px'
  },
  popover: {
    marginTop: 5,
    width: 128
  },
  triangle: {
    ...triangleTop,
    right: 11
  }
};
