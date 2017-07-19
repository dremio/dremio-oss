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
import Immutable  from 'immutable';
import PureRender from 'pure-render-decorator';
import Radium from 'radium';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';

import Menu from 'components/Menus/Menu';
import MenuItem from 'components/Menus/MenuItem';
import FontIcon from 'components/Icon/FontIcon';

import { body } from 'uiTheme/radium/typography';

@Radium
@PureRender
export default class SortBySelect extends Component {

  static propTypes = {
    activeFilter: PropTypes.string,
    defaultLabel: PropTypes.string,
    id: PropTypes.string.isRequired,
    inProgress: PropTypes.bool.isRequired,
    orderedColumn: PropTypes.instanceOf(Immutable.Map).isRequired,
    customStyles: PropTypes.object,
    value: PropTypes.string,
    options: PropTypes.array
  };

  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
    this.handleTouchTap = this.handleTouchTap.bind(this);
    this.state = {
      value: props.value,
      label: '',
      open: false
    };
  }

  getSelectedOption() {
    return this.state.value;
  }

  handleChange(value, label) {
    this.setState({value, label, open: false});
  }

  handleTouchTap(e) {
    this.setState({
      open: true,
      anchorEl: e.currentTarget
    });
  }

  handleRequestClose() {
    this.setState({open: false});
  }

  render() {
    const { options, defaultLabel, customStyles, id } = this.props;
    const mappedOptions = options.map((item, index) => {
      return  (
        <MenuItem className='menu-item-inner'
          key={`${item.label}_${index}`}
          onTouchTap={this.handleChange.bind(this, item.value, item.label)}>
          {item.label}
        </MenuItem>
      );
    });

    return (
      <div style={[styles.main, customStyles, body]} className={id}
        onClick={this.handleTouchTap}>
        {`${defaultLabel}: ${this.state.label}`}
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{horizontal: 'left', vertical: 'bottom'}}
          targetOrigin={{horizontal: 'left', vertical: 'top'}}
          onRequestClose={this.handleRequestClose.bind(this)}
          animation={PopoverAnimationVertical}>
          <Menu>{mappedOptions}</Menu>
        </Popover>
        <FontIcon type='ArrowDownSmall' theme={styles.arrowStyle}/>
      </div>
    );
  }
}

const styles = {
  main: {
    display: 'flex',
    alignItems: 'center',
    position: 'relative',
    margin: '0 10px 0 0',
    padding: '0 20px 0 0',
    cursor: 'pointer'
  },
  arrowStyle: {
    Container: {
      position: 'absolute',
      right: 0,
      top: '-2px',
      bottom: 0,
      margin: 'auto',
      height: 23
    }
  }
};
