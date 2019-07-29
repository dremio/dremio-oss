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
import { Component, PureComponent } from 'react';
import { connect } from 'react-redux';
import classNames from 'classnames';
import Radium from 'radium';
import Immutable  from 'immutable';

import PropTypes from 'prop-types';

import EntityLink from '@app/pages/HomePage/components/EntityLink';
import { EntityIcon } from '@app/pages/HomePage/components/EntityIcon';
import { EntityName } from '@app/pages/HomePage/components/EntityName';
import { Popover, MouseEvents } from '@app/components/Popover';

import AllSpacesMenu from 'components/Menus/HomePage/AllSpacesMenu';
import AllSourcesMenu from 'components/Menus/HomePage/AllSourcesMenu';
import { ENTITY_TYPES } from '@app/constants/Constants';
import { getRootEntityTypeByIdV3 } from '@app/selectors/home';
import ContainerDatasetCountV3, { ContainerDatasetCount } from '@app/pages/HomePage/components/ContainerDatasetCount';

import ResourcePin from './ResourcePin';
import EllipsedText from './EllipsedText';
import './FinderNavItem.less';

const mapStateToPropsV3 = (state, { entityId }) => {
  const type = getRootEntityTypeByIdV3(state, entityId);
  const props = {
    entityType: type
  };

  return props;
};

// a component that would be used for V3 api responses
@connect(mapStateToPropsV3)
export class FinderNavItemV3 extends PureComponent {
  static propTypes = {
    //public api
    entityId: PropTypes.string.isRequired,
    // connected
    entityType: PropTypes.oneOf([ENTITY_TYPES.home, ENTITY_TYPES.source, ENTITY_TYPES.space]).isRequired
  };

  render() {
    const { entityId } = this.props;

    return (
      <EntityLink entityId={entityId} activeClassName='active' className='finder-nav-item-link'>
        <EntityIcon entityId={entityId} />
        <EntityName entityId={entityId} style={{marginRight: 5}} />
        <ContainerDatasetCountV3 entityId={entityId} />
        {entityId && <ResourcePin entityId={entityId} />}
      </EntityLink>
    );
  }
}

const mapStateToProps = (state, { item }) => ({
  entityType: getRootEntityTypeByIdV3(state, item.id)
});

@Radium
export class FinderNavItem extends Component {
  static propTypes = {
    item: PropTypes.object.isRequired,
    style: PropTypes.object,

    //connected
    entityType: PropTypes.oneOf([ENTITY_TYPES.home, ENTITY_TYPES.source, ENTITY_TYPES.space]).isRequired

  };

  constructor() {
    super();
    this.state = {
      menuOpen: false
    };
    this.lastMouseEventPosition = null;
  }

  handleRightClick = (e) => {
    // home space does not have context menu
    if (!this.hasMenu()) return;

    e.preventDefault();
    this.lastMouseEventPosition = this.rightClickPosition(e);
    this.setState({
      menuOpen: true,
      anchorEl: e.currentTarget
    });
  };

  hasMenu = () => this.props.entityType !== ENTITY_TYPES.home;

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
      return <AllSpacesMenu spaceId={item.id} closeMenu={this.handleMenuClose}/>;
    case ENTITY_TYPES.source:
      return <AllSourcesMenu item={Immutable.fromJS(item)} closeMenu={this.handleMenuClose}/>;
    default:
      return null;
    }
  };

  render() {
    const { style, entityType } = this.props;
    const {
      id,
      name,
      numberOfDatasets,
      disabled,
      datasetCountBounded
    } = this.props.item;
    const itemClass = classNames('finder-nav-item');

    return (
      <li className={itemClass} style={[disabled && styles.disabled, style]} onContextMenu={this.handleRightClick}>
        {
            entityType === ENTITY_TYPES.space ?
              <FinderNavItemV3 entityId={id} /> :
              (
                <EntityLink entityId={id} activeClassName='active' className='finder-nav-item-link'>
                  <EntityIcon entityId={id} />
                  <EllipsedText text={name} style={{marginRight: 5}} />
                  <ContainerDatasetCount count={numberOfDatasets} isBounded={datasetCountBounded} />
                  {id && <ResourcePin entityId={id} />}
                </EntityLink>
              )
        }
        {this.hasMenu() && this.state.menuOpen &&
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

export default connect(mapStateToProps)(FinderNavItem);

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
