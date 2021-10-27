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

import React, { useState, useRef } from 'react';
import {useIntl} from 'react-intl';
import PropTypes from 'prop-types';
import classNames from 'classnames';

import './SideNav.less';
import './SideNavHoverMenu.less';

const SideNavHoverMenu = (props) => {
  const {wideNarrowWidth, tooltipStringId, tooltipString, menu, icon, divBlob, menuDisplayUp, isActive} = props;

  const intl = useIntl();
  const linkBtnRef = useRef();
  const menuRef = useRef();
  const [popMenuExtraClass, popupMenuClass] = useState(' --hide');
  const showPopupWaitTime = 250;
  const hidePopupWaitTime = 250;

  const displayTooltip = wideNarrowWidth === ' --narrow';
  const displayLabel = wideNarrowWidth === ' --wide' ? ' --show' : ' --hide';
  const menuPosition = wideNarrowWidth === ' --wide' ? ' --wide' : ' --narrow';
  const menuLinkMenuDisplayed = popMenuExtraClass === ' --show' ? ' --menuDisplayed' : '';
  const isActiveStyle = isActive ? ' --active' : '';

  const mouseEnter = () => {
    if (popMenuExtraClass === ' -show') {
      return;
    }

    if (linkBtnRef.current && linkBtnRef.current.hideTimer) {
      clearTimeout(linkBtnRef.current.hideTimer);
      linkBtnRef.current.hideTimer = null;
    }

    linkBtnRef.current.showTimer = setTimeout(() => {
      popupMenuClass(' --show');
      if (linkBtnRef.current) {
        linkBtnRef.current.showTimer = null;
      }
    }, showPopupWaitTime);
  };

  const mouseLeave = () => {
    if (linkBtnRef.current && linkBtnRef.current.showTimer) {
      clearTimeout(linkBtnRef.current.showTimer);
      linkBtnRef.current.showTimer = null;
      popupMenuClass(' --hide');

      //return;
    }

    linkBtnRef.current.hideTimer = setTimeout(() => {
      popupMenuClass(' --hide');
      if (linkBtnRef.current) {
        linkBtnRef.current.hideTimer = null;
      }
    }, hidePopupWaitTime);
  };


  const closeMenu = () => {
  };

  // adjust the position of the menu
  let menuAdjustment = 0;
  if (menuDisplayUp) {
    menuAdjustment = linkBtnRef.current ? linkBtnRef.current.offsetTop : 0;
    menuAdjustment -= menuRef.current ? menuRef.current.clientHeight : 0;
    menuAdjustment = {top: menuAdjustment + 64 + 'px'};
  } else {
    menuAdjustment = linkBtnRef.current ? linkBtnRef.current.offsetTop : 0;
    menuAdjustment = {top: menuAdjustment + 'px'};
  }

  // get the tooltip
  const stringObj = {};
  if (tooltipStringId) {
    stringObj.id = tooltipStringId;
  }
  const tooltip = tooltipString !== undefined ? tooltipString : intl.formatMessage(stringObj);

  return (
    <div className={classNames('sideNav-item', wideNarrowWidth, isActiveStyle)} ref={linkBtnRef}>
      <div
        className={classNames('sideNav-item__hoverMenu', wideNarrowWidth, isActiveStyle, menuLinkMenuDisplayed)}
        onMouseEnter={(e) => mouseEnter(e)}
        onMouseLeave={(e) => mouseLeave(e)}
        title={displayTooltip ? tooltip : ''}
      >
        <div className={classNames('sideNav-items', wideNarrowWidth)}>
          {icon &&
            <>
              <div className='sideNav-item__icon'>
                <span className={classNames('sideNav-item__iconType', icon)}></span>
              </div>
              <div className={'sideNav-item__labelNext' + displayLabel}>
                {tooltip}
              </div>
            </>
          }
          {divBlob &&
            <>
              {divBlob}
            </>
          }
        </div>
      </div>
      <div ref={menuRef}
        className={classNames('sideNav-menu', popMenuExtraClass, menuPosition)}
        style={{...menuAdjustment}}
        onMouseEnter={(e) => mouseEnter(e)}
        onMouseLeave={(e) => mouseLeave(e)}
      >
        {React.cloneElement(menu, {closeMenu})}
      </div>
    </div>
  );
};

SideNavHoverMenu.propTypes = {
  wideNarrowWidth: PropTypes.string.isRequired,
  tooltipStringId: PropTypes.string,
  tooltipString: PropTypes.string,
  menu: PropTypes.object.isRequired,
  icon: PropTypes.string,
  divBlob: PropTypes.object,
  isActive: PropTypes.bool,
  menuDisplayUp: PropTypes.bool
};


export default SideNavHoverMenu;
