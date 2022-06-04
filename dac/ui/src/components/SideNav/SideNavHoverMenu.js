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
import { Tooltip } from 'dremio-ui-lib';
import Art from '@app/components/Art';

import './SideNav.less';
import './SideNavHoverMenu.less';

const SideNavHoverMenu = (props) => {
  const  {tooltipStringId, tooltipString, menu, icon, divBlob, menuDisplayUp, isActive} = props;

  const intl = useIntl();
  const linkBtnRef = useRef();
  const menuRef = useRef();
  const [popMenuExtraClass, popupMenuClass] = useState(' --hide');
  const [showPopup, setShowPopup] = useState(false);
  const showPopupWaitTime = 250;
  const hidePopupWaitTime = 250;

  const menuPosition = '--narrow';
  const menuLinkMenuDisplayed = popMenuExtraClass === ' --show' ? ' --menuDisplayed' : '';

  const mouseEnter = () => {
    if (popMenuExtraClass === ' -show') {
      return;
    }

    if (linkBtnRef.current && linkBtnRef.current.hideTimer) {
      clearTimeout(linkBtnRef.current.hideTimer);
      linkBtnRef.current.hideTimer = null;
    }

    linkBtnRef.current.showTimer = setTimeout(() => {
      setShowPopup(true);
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
      setShowPopup(false);

      //return;
    }

    linkBtnRef.current.hideTimer = setTimeout(() => {
      popupMenuClass(' --hide');
      setShowPopup(false);
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
    <div className={classNames('sideNav-item', isActive)} ref={linkBtnRef}>
      <Tooltip title={tooltip}>
        <div
          className={classNames('sideNav-item__hoverMenu', isActive, menuLinkMenuDisplayed)}
          onMouseEnter={(e) => mouseEnter(e)}
          onMouseLeave={(e) => mouseLeave(e)}
        >
          <div className='sideNav-items'>
            {icon &&
              <div className='sideNav-item__icon'>
                <Art src={icon} alt={tooltip}  />
              </div>
            }
            {divBlob && divBlob}
          </div>
        </div>
      </Tooltip>
      {showPopup && <div ref={menuRef}
        className={classNames('sideNav-menu', popMenuExtraClass, menuPosition)}
        style={{...menuAdjustment}}
        onMouseEnter={(e) => mouseEnter(e)}
        onMouseLeave={(e) => mouseLeave(e)}
      >
        {React.cloneElement(menu, {closeMenu})}
      </div>}
    </div>
  );
};

SideNavHoverMenu.propTypes = {
  tooltipStringId: PropTypes.string,
  tooltipString: PropTypes.string,
  menu: PropTypes.object.isRequired,
  icon: PropTypes.string,
  divBlob: PropTypes.object,
  isActive: PropTypes.string,
  menuDisplayUp: PropTypes.bool
};


export default SideNavHoverMenu;
