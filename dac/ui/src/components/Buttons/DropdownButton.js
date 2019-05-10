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
import React, { Component } from 'react';
import Radium from 'radium';
import PropTypes from 'prop-types';
import deepEqual from 'deep-equal';

import { SelectView } from '@app/components/Fields/SelectView';
import { SECONDARY, SECONDARY_BORDER, BLUE } from 'uiTheme/radium/colors';
import FontIcon from 'components/Icon/FontIcon';

// todo: use 'uiTheme/radium/buttons' instead
const PRIMARY_BASE = {
  backgroundColor: BLUE,
  borderBottom: '1px solid #3399A8'
};

@Radium
export default class DropdownButton extends Component {
  static propTypes = {
    className: PropTypes.string,
    menu: PropTypes.node,
    iconType: PropTypes.string,
    action: PropTypes.func,
    defaultValue: PropTypes.object,
    disabled: PropTypes.bool,
    type: PropTypes.string,
    iconStyle: PropTypes.object,
    shouldSwitch: PropTypes.bool.isRequired,
    hideDropdown: PropTypes.bool.isRequired
  };

  static defaultProps = {
    shouldSwitch: true,
    hideDropdown: false
  }

  componentWillMount() {
    this.receiveProps(this.props);
  }

  componentWillReceiveProps(nextProps) {
    this.receiveProps(nextProps, this.props);
  }

  receiveProps(newProps, oldProps = {}) {
    if (!deepEqual(newProps.defaultValue, oldProps.defaultValue)) {
      this.setState({
        text: newProps.defaultValue.label,
        name: newProps.defaultValue.name
      });
    }
  }

  doAction = (closeDD, item) => {
    if (this.props.shouldSwitch) {
      this.setState({
        text: item.label,
        name: item.name
      });
    }

    closeDD();
    this.props.action(item.name);
  }

  action = (e) => {
    if (!this.props.disabled) {
      this.props.action(this.state.name, e);
    }
  }

  render() {
    const { className, type, disabled, iconType, hideDropdown } = this.props;
    //const background = { backgroundColor: '#68C6D3' };
    const base = {...styles.base, ...(type === 'primary' ? PRIMARY_BASE : {})};
    const bodyFont = {...(type === 'primary' ? {color: '#fff'} : {})};
    const divider = {...(type === 'primary' ? {borderLeft: '1px solid rgba(255,255,255,0.25)'} : {})};
    const hoverStyle = {backgroundColor: type === 'primary' ? 'rgba(255,255,255,0.2)' : 'rgba(255,255,255,0.2)'};

    const toggleIcontype = type === 'primary'
      ? 'Arrow-Down-Small-Reversed'
      : 'Arrow-Down-Small';

    const isButtonHovered = !disabled ? Radium.getState(this.state, 'button', ':hover') : false;
    const isTogglerHovered = !disabled ? Radium.getState(this.state, 'toggler', ':hover') : false;

    const dividerStyle = isButtonHovered || isTogglerHovered ? {
      height: '100%',
      left: 0,
      top: 0,
      marginTop: 0,
      opacity: '.75'
    } : {};
    const cursorStyle = !disabled ? {} : { cursor: 'default' };

    return (
      <div className={className} style={[base, cursorStyle, disabled && {opacity: 0.7}]}>
        <div key='button' onClick={this.action} style={[styles.mainButton, cursorStyle, isButtonHovered && hoverStyle]}>
          <div style={[styles.iconWrap, !iconType ? {display: 'none'} : {}]}>
            <FontIcon
              type={iconType}
              theme={{...styles.icon, ...this.props.iconStyle}}/>
          </div>
          <div style={[styles.text, cursorStyle]} key='text'>
            <span style={[{}, bodyFont, {margin: 'auto'}]}>
              {this.state.text}
            </span>
          </div>
        </div>
        {
          !hideDropdown && <SelectView
            listStyle={{overflow: 'visible', marginTop: 7}}
            content={
              <div
                style={[styles.toggler, isTogglerHovered && hoverStyle, cursorStyle]}
                key='toggler'>
                <div style={[styles.divider, dividerStyle, divider]} />
                <FontIcon
                  type={toggleIcontype}
                  theme={{Container: {width: 24, height: 24}}}
                  onClick={disabled ? undefined : this.toggleDropdown}/>
              </div>
            }
            hideExpandIcon
            listRightAligned
            useLayerForClickAway={false}
          >
            {
              ({ closeDD }) => (
                <div style={styles.popover}>
                  <div style={styles.triangle}/>
                  {
                    React.cloneElement(this.props.menu, {
                      action: this.doAction.bind(this, closeDD)
                    })
                  }
                </div>
              )
            }
          </SelectView>
        }
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    backgroundColor: SECONDARY,
    borderBottom: `1px solid ${SECONDARY_BORDER}`,
    borderRadius: 2,
    minWidth: 100,
    height: 28
  },
  mainButton: {
    display: 'flex',
    flex: 1,
    alignItems: 'center',
    cursor: 'pointer',
    height: 28,
    ':hover': {} // for Radium.getState
  },
  triangle: {
    width: 0,
    height: 0,
    borderStyle: 'solid',
    borderWidth: '0 4px 6px 4px',
    borderColor: 'transparent transparent #fff transparent',
    position: 'absolute',
    zIndex: 99999,
    right: 6,
    top: -6
  },
  popover: {
    padding: 0
  },
  text: {
    width: '100%',
    height: '100%',
    display: 'flex',
    alignItems: 'center',
    flex: 1,
    textAlign: 'center',
    position: 'relative',
    padding: '0 6px'
  },
  toggler: {
    display: 'flex',
    position: 'relative',
    justifyContent: 'center',
    borderRadius: 2,
    alignItems: 'center',
    width: 20,
    height: 27,
    cursor: 'pointer',
    ':hover': {} // for Radium.getState
  },
  divider: {
    height: 20,
    left: 0,
    top: '50%',
    marginTop: -10,
    position: 'absolute',
    borderLeft: '1px solid #c2cdd5',
    backgroundColor: 'rgba(255,255,255,0.25)',
    display: 'block'
  },
  iconWrap: {
    paddingLeft: 6,
    height: 27,
    display: 'flex',
    position: 'relative',
    justifyContent: 'center',
    alignItems: 'center',
    borderRadius: 2
  },
  icon: {
    Icon: {
      width: 17,
      height: 17
    },
    Container: {
      width: 17,
      height: 17,
      display: 'block'
    }
  }
};
