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
import PropTypes from 'prop-types';
import { Link } from 'react-router';
import Immutable  from 'immutable';

import FinderNavItem from 'components/FinderNavItem';

export default class FinderNavSection extends Component {

  static propTypes = {
    items: PropTypes.instanceOf(Immutable.List).isRequired,
    isInProgress: PropTypes.bool,
    maxItemsCount: PropTypes.number,
    listHref: PropTypes.oneOfType([PropTypes.string, PropTypes.object])
  };

  render() {
    const { items, maxItemsCount, listHref } = this.props;
    if (!items.size) return null;

    const hasMore = items.size > maxItemsCount;
    return (
      <div
        className='holder'
        style={styles.base}>
        <ul className='visible-items'>
          {items.map((item, index) => {
            if (index < maxItemsCount) {
              return (
                <FinderNavItem
                  key={item.get('id')}
                  item={item.toJS()}
                  itemHasContextMenu // both spaces and sources nav items have context menu
                />
              );
            }
          })}
        </ul>
        {hasMore && ( //todo: loc
          <Link className='show-more-btn' to={listHref}>
            {`Show All (${items.size}) »`}
          </Link>
        )}
      </div>
    );
  }
}

const styles = {
  base: {
    position: 'relative',
    minHeight: '1.5em' // leave a little space while loading
  },
  loader: {
    position: 'absolute',
    width: '100%',
    height: '100%',
    paddingTop: 10,
    backgroundColor: 'rgba(255,255,255,0.9)'
  }
};
