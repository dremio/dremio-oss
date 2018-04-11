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
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';

@PureRender
@Radium
export default class SelectView extends Component {
  static propTypes = {
    inProgress: PropTypes.bool,
    getDropDown: PropTypes.func.isRequired,
    id: PropTypes.string,
    label: PropTypes.string.isRequired
  };

  constructor(props) {
    super(props);
    this.state = {
      open: false
    };
  }

  handleTouchTap(e) {
    this.setState({
      open: true,
      anchorEl: e.currentTarget
    });
  }

  handleRequestClose() {
    this.setState({
      open: false
    });
  }

  render() {
    const { getDropDown, label, id } = this.props;
    return (
      <div style={style.base} className={id}>
        <div onTouchTap={this.handleTouchTap.bind(this)} style={style.label} >
          {label}
          <FontIcon type='ArrowDownSmall' theme={style.arrow}/>
        </div>
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{horizontal: 'left', vertical: 'bottom'}}
          targetOrigin={{horizontal: 'left', vertical: 'top'}}
          onRequestClose={this.handleRequestClose.bind(this)}
          animation={PopoverAnimationVertical}>
          {getDropDown()}
        </Popover>
      </div>
    );
  }
}

const style = {
  base: {
    margin: 0,
    height: 38,
    display: 'flex',
    alignItems: 'center'
  },
  arrow: {
    Container: {
      position: 'relative',
      top: 1
    }
  },
  label: {
    cursor: 'pointer',
    color: '#a4a4a4',
    padding: 0,
    display: 'flex',
    alignItems: 'center'
  }
};
