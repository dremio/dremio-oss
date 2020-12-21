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
import {PureComponent, Fragment} from 'react';
import PropTypes from 'prop-types';
import FilterSelectMenuWrapper from '@app/components/Fields/FilterSelectMenuWrapper';
import EnginesFilterMixin from 'dyn-load/pages/AdminPage/subpages/Provisioning/components/EnginesFilterMixin';

import {
  ENGINE_FILTER_NAME,
  ENGINE_FILTER_LABEL,
  DEFAULT_ENGINE_FILTER_SELECTIONS
} from 'dyn-load/constants/provisioningPage/provisioningConstants';
//import { styles } from '@app/pages/HomePage/components/MainInfo';

@EnginesFilterMixin
export default class EnginesFilter extends PureComponent {
  static propTypes = {
    filterState: PropTypes.shape({
      filters: PropTypes.object //key: filter type, value: array of selected filter values
    }),
    onUpdateFilterState: PropTypes.func,
    style: PropTypes.object
  };

  onUpdated = (newFilters) => {
    this.props.onUpdateFilterState({filters: newFilters});
  };

  getShownFilters = () => {
    return Object.values(ENGINE_FILTER_NAME);
  };

  renderSelectMenu = (menuType) => {
    const {filterState} = this.props;
    return (
      <FilterSelectMenuWrapper
        filterStateFilters={filterState.filters}
        menuType={menuType}
        filterItems={this.getFilterItems()}
        filterLabels={ENGINE_FILTER_LABEL}
        defaultFilterSelections={DEFAULT_ENGINE_FILTER_SELECTIONS}
        onUpdateFilterState={this.onUpdated.bind(this)}
      />
    );
  };

  render() {
    const shownFilters = this.getShownFilters();
    const {filterState} = this.props;

    return (
      <div style={{display: 'flex', flexDirection: 'row', justifyContent: 'space-between'}}>
        <div style={{...styles.base, ...this.props.style}}>
          {shownFilters.map((filterName, idx) => {
            return <Fragment key={idx}>
              {this.renderSelectMenu(filterName)}
              {(idx < (shownFilters.length - 1)) && <div style={styles.divider}/>}
            </Fragment>;
          })}
        </div>
        <div style={styles.searchBox}>
          {this.renderSearch(filterState.filters, this.onUpdated.bind(this))}
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'flex-start',
    alignItems: 'center',
    height: 33,
    paddingTop: 6
  },
  searchBox: {
    display: 'flex',
    justifyContent: 'flex-end',
    alignItems: 'center',
    height: 33,
    paddingTop: 6
  },
  divider: {
    backgroundColor: '#fff',
    height: 16,
    width: 16
  }
};
