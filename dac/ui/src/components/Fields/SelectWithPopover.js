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
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FontIcon from 'components/Icon/FontIcon';
import RaisedButton from 'material-ui/RaisedButton';
import { Popover, PopoverAnimationVertical } from 'material-ui/Popover';
import Menu from 'material-ui/Menu';
import MenuItem from 'material-ui/MenuItem';

import { DIVIDER, PALE_GREY } from 'uiTheme/radium/colors';
import { formDefault } from 'uiTheme/radium/typography';

const MAX_HEIGHT = 236;

@Radium
@PureRender
export default class SelectWithPopover extends Component {
  static propTypes = {
    onChange: PropTypes.func,
    items: PropTypes.arrayOf(
      PropTypes.shape({
        label: PropTypes.string,
        icon: PropTypes.string,
        des: PropTypes.string
      })
    ),
    curItem: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.node
    ]),
    styleForButton: PropTypes.object,
    styleForDropdown: PropTypes.object,
    styleWrap: PropTypes.object,
    dataQa: PropTypes.string
  };

  static defaultProps = {
    items: []
  }

  constructor(props) {
    super(props);
    this.handleTouchTap = this.handleTouchTap.bind(this);
    this.handleRequestClose = this.handleRequestClose.bind(this);
    this.onChange = this.onChange.bind(this);

    this.state = { open: false };
  }

  onChange(e, item) {
    this.handleRequestClose();
    this.props.onChange(item);
  }

  handleTouchTap(e) {
    e.preventDefault();
    this.setState({ open: true, anchorEl: e.currentTarget });
  }

  handleRequestClose() {
    this.setState({ open: false });
  }

  renderItems(items) {
    return items.map((item, index) => {
      const { label, value, icon, des } = item;
      const itemIcon = icon ? <FontIcon type={icon}/> : null;
      const primaryText = (
        <div style={styles.defaultTextStyle}>
          <div style={{ height: 23 }}>{itemIcon}</div>
          <div>{label}</div>
        </div>
      );

      const divider = index !== items.length - 1 ? { borderBottom: `1px solid ${DIVIDER}` } : {};
      const secondaryText = <div style={styles.secondaryText}>{des}</div>;
      return (
        <MenuItem
          key={index}
          data-qa={label}
          value={value}
          primaryText={primaryText}
          secondaryText={secondaryText}
          style={{ width: 223 }}
          innerDivStyle={{ ...formDefault, ...styles.wrapped, ...divider }}/>
      );
    });
  }

  render() {
    const { styleForButton, styleWrap, styleForDropdown, curItem, dataQa } = this.props;

    return (
      <div style={styleWrap || {}}>
        <RaisedButton
          data-qa={dataQa}
          onTouchTap={this.handleTouchTap}
          style={{ ...styles.button, ...styleForButton }}
          backgroundColor={PALE_GREY}>
          {curItem}
        </RaisedButton>
        <Popover
          open={this.state.open}
          anchorEl={this.state.anchorEl}
          anchorOrigin={{ horizontal: 'left', vertical: 'bottom' }}
          targetOrigin={{ horizontal: 'left', vertical: 'top' }}
          onRequestClose={this.handleRequestClose}
          animation={PopoverAnimationVertical}>
          <Menu onChange={this.onChange} maxHeight={MAX_HEIGHT} listStyle={styleForDropdown || {}}>
            {this.renderItems(this.props.items)}
          </Menu>
        </Popover>
      </div>
    );
  }
}

const styles = {
  button: {
    height: 24,
    width: 130,
    boxShadow: 0
  },
  secondaryText: {
    whiteSpace: 'normal',
    marginTop: -5,
    lineHeight: '14px',
    marginLeft: 24,
    color: '#999999'
  },
  wrapped: {
    marginLeft: -15,
    lineHeight: 2,
    height: 55,
    display: 'flex',
    justifyContent: 'flex-end',
    alignItems: 'flex-start',
    flexDirection: 'column-reverse'
  },
  defaultTextStyle: {
    display: 'flex',
    alignItems: 'center',
    height: 27
  }
};
