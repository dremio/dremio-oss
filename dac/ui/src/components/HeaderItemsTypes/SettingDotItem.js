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
import { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';

import { bodySmall } from 'uiTheme/radium/typography';
import { EXPLORE_SQL_BUTTON_COLOR } from 'uiTheme/radium/colors.js';
import FontIcon from 'components/Icon/FontIcon';
import ChangeContextMenu from 'components/Menus/ExplorePage/ChangeContextMenu';


@Radium
export default class SettingDotItem extends Component {
  static propTypes = {
    style: PropTypes.object
  }

  constructor(props) {
    super(props);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.handleTouchTap = this.handleTouchTap.bind(this);
    this.state = {
      open: false
    };
  }

  handleRequestClose() {
    this.setState({
      open: false
    });
  }

  handleTouchTap(event) {
    this.setState({
      open: true,
      anchorEl: event.currentTarget
    });
  }

  render() {
    return (
      <div>
        <div style={[styles.main, bodySmall, this.props.style]} onClick={this.handleTouchTap}>
          <FontIcon type='Ellipsis' theme={styles.ellipsis}/>
          <FontIcon type='Arrow-Down-Small' theme={{Container: {position: 'relative', top: 2}}} />
        </div>
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{horizontal: 'left', vertical: 'bottom'}}
          targetOrigin={{horizontal: 'left', vertical: 'top'}}
          onRequestClose={this.handleRequestClose}
          animation={PopoverAnimationVertical}>
          <ChangeContextMenu />
        </Popover>
      </div>
    );
  }
}

const styles = {
  main: {
    backgroundColor: 'transparent',
    borderTop: 'none',
    borderBottom: 'none',
    borderLeft: 'none',
    borderRight: 'none',
    minWidth: 60,
    margin: 0,
    paddingRight: 8,
    paddingLeft: 8,
    borderRadius: 2,
    width: 55,
    height: 26,
    display: 'flex',
    justifyContent: 'space-around',
    alignItems: 'center',
    color: 'gray',
    cursor: 'pointer',
    ':hover': {
      backgroundColor: EXPLORE_SQL_BUTTON_COLOR
    }
  },
  ellipsis: {
    Container: {
      display: 'flex',
      alignItems: 'center'
    }
  }
};
