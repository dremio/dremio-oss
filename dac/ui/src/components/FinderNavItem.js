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
import { PropTypes, Component } from 'react';
import { Link } from 'react-router';
import classNames from 'classnames';
import Radium from 'radium';

import ResourcePin from 'components/ResourcePin';
import FontIcon from 'components/Icon/FontIcon';
import { getIconStatusDatabase } from 'utils/iconUtils';
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
    const MAX_CHART = 19;
    const linkText = name && name.length > MAX_CHART ? name.slice(0, MAX_CHART) + '...' : name;
    let count;
    if (numberOfDatasets !== undefined) {
      count = numberOfDatasets;
    } else if (datasetCount !== undefined) {
      count = datasetCount;
    }
    const typeIcon = iconClass && !state ? iconClass : getIconStatusDatabase(state.status);

    return (
      <li className={itemClass} style={[disabled && styles.disabled, style]}>
        <Link to={links.self} activeClassName='active' onlyActiveOnIndex>
          <span className='finder-nav-item-link'>
            <FontIcon
              type={typeIcon}
              theme={styles.iconStyle}/>
            {linkText}
          </span>
          <div className='count-wrap'>
            {count !== undefined ? <span className='count'>{count}</span> : null}
            {toggleActivePin
              ? (
                <ResourcePin
                  name={name}
                  isActivePin={isActivePin || false}
                  toggleActivePin={toggleActivePin} />
              )
              : null }
          </div>
        </Link>
      </li>
    );
  }
}

const styles = {
  iconStyle: {
    Container: {
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
