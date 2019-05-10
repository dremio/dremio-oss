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
import classNames from 'classnames';
import Radium from 'radium';
import Immutable  from 'immutable';

import PropTypes from 'prop-types';

import EntityLink from '@app/pages/HomePage/components/EntityLink';
import { Popover, MouseEvents } from '@app/components/Popover';

import { getIconStatusDatabase } from 'utils/iconUtils';
import AllSpacesMenu from 'components/Menus/HomePage/AllSpacesMenu';
import AllSourcesMenu from 'components/Menus/HomePage/AllSourcesMenu';
import { ENTITY_TYPES } from '@app/constants/Constants';

import ResourcePin from './ResourcePin';
import FontIcon from './Icon/FontIcon';
import EllipsedText from './EllipsedText';
import './FinderNavItem.less';

@Radium
export default class FinderNavItem extends Component {
  static propTypes = {
    item: PropTypes.object.isRequired,
    style: PropTypes.object,
    itemHasContextMenu: PropTypes.bool
  };

  constructor() {
    super();
    this.state = {
      menuOpen: false
    };
    this.lastMouseEventPosition = null;
  }

  handleRightClick = (e) => {
    if (!this.props.itemHasContextMenu) return;

    e.preventDefault();
    this.lastMouseEventPosition = this.rightClickPosition(e);
    this.setState({
      menuOpen: true,
      anchorEl: e.currentTarget
    });
  };

  // make position string for comparing mouse events
  rightClickPosition = (e) => `x: ${e.clientX}, y: ${e.clientY}`;
  clickAwayPosition = (e) => `x: ${e.x}, y: ${e.y}`;

  handleMenuClose = () => {
    this.setState({menuOpen: false});
  };

  getMenu = () => {
    const { item } = this.props;
    switch (item.entityType) {
    case ENTITY_TYPES.space:
      return <AllSpacesMenu item={Immutable.fromJS(item) } closeMenu={this.handleMenuClose}/>;
    case ENTITY_TYPES.source:
      return <AllSourcesMenu item={Immutable.fromJS(item)} closeMenu={this.handleMenuClose}/>;
    default:
      return null;
    }
  };

  render() {
    const { style, itemHasContextMenu } = this.props;
    const {
      id,
      name,
      iconClass,
      numberOfDatasets,
      datasetCount,
      readonly,
      active,
      disabled,
      state,
      datasetCountBounded
    } = this.props.item;
    const itemClass = classNames('finder-nav-item',
      { readonly },
      { active }
    );
    let count = null;
    let hoverText = null;

    if (numberOfDatasets !== undefined) {
      hoverText = la('Physical Dataset Count');

      if (datasetCountBounded) {
        if (numberOfDatasets === 0) {
          // we found nothing and were count/time bound, so display '-'
          count = la('-');
        } else {
          // we found some datasets and were count/time bound, so add '+' to the dataset number
          count = numberOfDatasets + la('+');
        }
      } else {
        count = numberOfDatasets;
      }
    } else if (datasetCount !== undefined) {
      // TODO: seems like dead code?
      count = datasetCount;
    }
    const typeIcon = iconClass && !state ? iconClass : getIconStatusDatabase(state.status);

    return (
      <li className={itemClass} style={[disabled && styles.disabled, style]} onContextMenu={this.handleRightClick}>
        <EntityLink entityId={id} activeClassName='active' className='finder-nav-item-link'>
          <FontIcon
            type={typeIcon}
            theme={styles.iconStyle}/>
          <EllipsedText text={name} style={{marginRight: 5}} />
          {count !== null && <span title={hoverText} className='count'>{count}</span>}
          {id && <ResourcePin entityId={id} />}
        </EntityLink>
        {itemHasContextMenu && this.state.menuOpen &&
          <Popover
            useLayerForClickAway={false}
            anchorEl={this.state.menuOpen ? this.state.anchorEl : null}
            listRightAligned
            onClose={this.handleMenuClose}
            clickAwayMouseEvent={MouseEvents.onMouseDown}
          >
            {this.getMenu()}
          </Popover>
        }
      </li>
    );
  }
}

const styles = {
  iconStyle: {
    Container: {
      height: 24,
      display: 'inline-block',
      verticalAlign: 'middle',
      marginRight: 5
    }
  },
  disabled: {
    opacity: 0.7,
    background: '#fff',
    pointerEvents: 'none',
    color: '#999'
  }
};
