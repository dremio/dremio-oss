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
import React, { PureComponent, Fragment } from 'react';
import PropTypes from 'prop-types';
import Radium from 'radium';
import classNames from 'classnames';

import { SelectView } from '@app/components/Fields/SelectView';
import FontIcon from '@app/components/Icon/FontIcon';

import { triangleTop } from 'uiTheme/radium/overlay';

@Radium
export default class DropdownMenu extends PureComponent {
  static propTypes = {
    dataQa: PropTypes.string,
    className: PropTypes.string,
    text: PropTypes.string,
    iconType: PropTypes.string,
    menu: PropTypes.node.isRequired,
    style: PropTypes.object,
    iconStyle: PropTypes.object,
    textStyle: PropTypes.object,
    hideArrow: PropTypes.bool,
    arrowStyle: PropTypes.object,
    hideDivider: PropTypes.bool,
    disabled: PropTypes.bool,
    isButton: PropTypes.bool,
    iconTooltip: PropTypes.string,
    fontIcon: PropTypes.string
  };

  static defaultProps = {
    text: '',
    iconType: ''
  };

  render() {
    const { dataQa, className, text, iconType, menu, style, iconStyle, textStyle, fontIcon,
      hideArrow, hideDivider, disabled, isButton, iconTooltip, arrowStyle } = this.props;

    const isTogglerHovered = !disabled ? Radium.getState(this.state, 'toggler', ':hover') : false;
    const hoverStyle = {backgroundColor: '#F9F9F9'};
    const togglerStyle = isButton ? styles.togglerButton : styles.toggler;
    const cursorStyle = disabled ? { cursor: 'default' } : { cursor: 'pointer'};
    const dividerStyle = isTogglerHovered || isTogglerHovered ? {
      height: '100%',
      left: 0,
      top: 0,
      marginTop: 0,
      opacity: '.75'
    } : {};

    const stdArrowStyle = isButton ? styles.downButtonArrow : styles.downArrow;

    return (
      <div style={isButton ? [styles.base, styles.button, style, isTogglerHovered && hoverStyle] : [styles.base, style]}>
        <SelectView
          content={
            <div className={classNames('dropdown-menu', className)} key='toggler' style={[togglerStyle, cursorStyle]}>
              {text && <span style={{...styles.text, ...textStyle}}>{text}</span>}
              {iconType &&
              <div style={styles.iconWrap}>
                <FontIcon
                  type={iconType}
                  tooltip={iconTooltip}
                  theme={{...styles.icon, ...iconStyle}}
                />
              </div>
              }
              {fontIcon &&
              <div >
                <div
                  className={fontIcon}
                  tooltip={iconTooltip}
                />
              </div>
              }
              {!hideDivider && <div style={[styles.divider, dividerStyle]} />}
              {!hideArrow && <i className='fa fa-angle-down' style={{...stdArrowStyle, ...arrowStyle}}/>}
            </div>
          }
          hideExpandIcon
          listStyle={styles.popover}
          listRightAligned
          dataQa={dataQa}
        >
          {
            ({ closeDD }) => (
              <Fragment>
                <div style={styles.triangle}/>
                {React.cloneElement(menu, { closeMenu: closeDD })}
              </Fragment>
            )
          }
        </SelectView>
      </div>
    );
  }

}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center'
  },
  button: {
    backgroundColor: '#F2F2F2',
    border: '1px solid #D9D9D9',
    borderRadius: 4,
    minWidth: 50,
    height: 32
  },
  togglerButton: {
    display: 'flex',
    position: 'relative',
    justifyContent: 'space-between',
    alignItems: 'center',
    height: 27,
    cursor: 'pointer',
    ':hover': {} // for Radium.getState
  },
  toggler: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center'
  },
  text: {
    margin: '0 6px 0 0'
  },
  iconWrap: {
    display: 'flex',
    flex: '1 1 0%',
    padding: '0 3px',
    height: 27,
    justifyContent: 'center',
    alignItems: 'center'
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
  },
  downArrow: {
    fontSize: '18px'
  },
  downButtonArrow: {
    fontSize: 14,
    color: '#77818F'
  },
  popover: {
    marginTop: 7,
    overflow: 'visible'
  },
  triangle: {
    ...triangleTop,
    right: 11
  },
  divider: {
    height: 20,
    margin: '0 4px',
    borderLeft: '1px solid #E5E5E5',
    display: 'block'
  }
};
