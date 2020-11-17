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
import { Component } from 'react';
import PropTypes from 'prop-types';
import DropdownMenu from '@app/components/Menus/DropdownMenu';

export default class HeaderDropdown extends Component {
  static propTypes = {
    dataQa: PropTypes.string,
    name: PropTypes.string,
    nameStyle: PropTypes.object,
    menu: PropTypes.node.isRequired,
    style: PropTypes.object,
    icon: PropTypes.string,
    tooltip: PropTypes.string,
    hideArrow: PropTypes.bool,
    arrowStyle: PropTypes.object,
    className: PropTypes.string
  };

  static defaultProps = {
    name: ''
  };

  render() {
    const { dataQa, name, nameStyle, menu, style, icon, tooltip, hideArrow, arrowStyle, className } = this.props;

    return (
      <DropdownMenu
        className={className}
        hideDivider
        dataQa={dataQa}
        style={{...styles.base, ...style}}
        menu={menu}
        hideArrow={hideArrow}
        arrowStyle={arrowStyle}
        textStyle={nameStyle}
        fontIcon={icon}
        text={name}
        iconToolTip={tooltip}
      />
    );
  }
}

const styles = {
  base: {
    padding: '0 10px'
  }
};
