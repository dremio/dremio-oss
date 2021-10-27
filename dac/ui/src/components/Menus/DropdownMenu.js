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
import React, { Fragment } from 'react';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import { SelectView } from '@app/components/Fields/SelectView';
import FontIcon from '@app/components/Icon/FontIcon';

import { triangleTop } from 'uiTheme/radium/overlay';

import './DropdownMenu.less';

const DropdownMenu = (props) => {

  const {
    dataQa,
    className,
    text,
    textTooltip,
    iconType,
    menu,
    style,
    iconStyle,
    textStyle,
    fontIcon,
    hideArrow,
    hideDivider,
    hideTopArrow,
    disabled,
    isButton,
    iconTooltip,
    arrowStyle,
    customItemRenderer,
    listStyle,
    customImage
  } = props;

  const togglerStyle = isButton ? 'dropdownMenu__togglerButton' : 'dropdownMenu__toggler';
  const cursorStyle = disabled ? { cursor: 'default' } : { cursor: 'pointer'};

  const stdArrowStyle = isButton ? styles.downButtonArrow : styles.downArrow;
  const lstStyle = listStyle ? listStyle : styles.popover;


  const selectedItemRenderer = () => (
    <>
      {text && <span className='dropdownMenu__text' style={{ ...textStyle}}>{text}</span>}

      {iconType &&
      <div className='dropdownMenu__iconWrap'>
        <FontIcon
          type={iconType}
          tooltip={iconTooltip}
          theme={{...styles.icon, ...iconStyle}}
        />
      </div>
      }
      {fontIcon &&
        <div>
          <div
            className={fontIcon}
            title={iconTooltip}
          />
        </div>
      }
      {customImage &&
        <div>
          {customImage}
        </div>
      }
    </>
  );

  return (
    <div className={classNames('dropdownMenu', isButton ? '--button' : '')} style={{...style}}>

      <SelectView
        content={
          <div className={classNames('dropdownMenu__content', className, togglerStyle)} key='toggler' style={{...cursorStyle}}  title={textTooltip}>

            {/* Use a custom look and feel if needed */}
            {customItemRenderer || selectedItemRenderer() }

            {!hideDivider && <div className='dropdownMenu__divider' />}
            {!hideArrow && <i className='fa fa-angle-down' style={{...stdArrowStyle, ...arrowStyle}}/>}
          </div>
        }
        hideExpandIcon
        listStyle={lstStyle}
        listRightAligned
        dataQa={dataQa}
      >
        {
          ({ closeDD }) => {
            return (
              <Fragment>
                {!hideTopArrow &&
                  <div style={styles.triangle}/>
                }
                {React.cloneElement(menu, { closeMenu: closeDD })}
              </Fragment>
            );
          }
        }
      </SelectView>
    </div>
  );
};

const styles = {
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
    fontSize: '18px',
    position: 'relative',
    top: '1px',
    marginLeft: '5px'
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

DropdownMenu.propTypes = {
  dataQa: PropTypes.string,
  className: PropTypes.string,
  text: PropTypes.string,
  iconType: PropTypes.string,
  menu: PropTypes.node.isRequired,
  style: PropTypes.object,
  iconStyle: PropTypes.object,
  textStyle: PropTypes.object,
  textTooltip: PropTypes.string,
  hideArrow: PropTypes.bool,
  hideTopArrow: PropTypes.bool,
  arrowStyle: PropTypes.object,
  hideDivider: PropTypes.bool,
  disabled: PropTypes.bool,
  isButton: PropTypes.bool,
  iconTooltip: PropTypes.string,
  fontIcon: PropTypes.string,
  customItemRenderer: PropTypes.element,
  customImage: PropTypes.object,
  listStyle: PropTypes.object
};

export default DropdownMenu;
