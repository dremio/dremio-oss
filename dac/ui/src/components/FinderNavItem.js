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
import { Link } from 'react-router';
import classNames from 'classnames';
import Radium from 'radium';

import PropTypes from 'prop-types';

import { getIconStatusDatabase } from 'utils/iconUtils';

import ResourcePin from './ResourcePin';
import FontIcon from './Icon/FontIcon';
import EllipsedText from './EllipsedText';
import './FinderNavItem.less';

@Radium
export default class FinderNavItem extends Component {
  static propTypes = {
    item: PropTypes.object.isRequired,
    toggleActivePin: PropTypes.func,
    style: PropTypes.object
  };
  render() {
    const { toggleActivePin, style } = this.props;
    const {
      name,
      iconClass,
      links,
      numberOfDatasets,
      datasetCount,
      readonly,
      isActivePin,
      active,
      disabled,
      state
    } = this.props.item;
    const itemClass = classNames('finder-nav-item',
      { readonly },
      { active }
    );
    let count;
    if (numberOfDatasets !== undefined) {
      count = numberOfDatasets;
    } else if (datasetCount !== undefined) {
      count = datasetCount;
    }
    const typeIcon = iconClass && !state ? iconClass : getIconStatusDatabase(state.status);

    return (
      <li className={itemClass} style={[disabled && styles.disabled, style]}>
        <Link onlyActiveOnIndex to={links.self} activeClassName='active' className='finder-nav-item-link'>
          <FontIcon
            type={typeIcon}
            theme={styles.iconStyle}/>
          <EllipsedText text={name} style={{marginRight: 5}} />
          {typeof count === 'number' && <span className='count'>{count}</span>}
          {toggleActivePin && (
            <ResourcePin
              name={name}
              isActivePin={isActivePin || false}
              toggleActivePin={toggleActivePin} />
          )}
        </Link>
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
