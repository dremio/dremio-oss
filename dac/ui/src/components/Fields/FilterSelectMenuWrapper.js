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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import {cloneDeep} from 'lodash/lang';
import FilterSelectMenu from 'components/Fields/FilterSelectMenu';

export default class FilterSelectMenuWrapper extends PureComponent {
  static propTypes = {
    filterStateFilters: PropTypes.object, //key: filter type, value: array of selected filter values
    menuType: PropTypes.string,
    filterItems: PropTypes.object,
    filterLabels: PropTypes.object,
    defaultFilterSelections: PropTypes.object,
    onUpdateFilterState: PropTypes.func
  };

  getValues = () => {
    const { filterStateFilters, defaultFilterSelections, menuType } = this.props;
    return filterStateFilters[menuType] || defaultFilterSelections[menuType];
  };

  addInfoToFilter = (value) => {
    const { filterStateFilters, onUpdateFilterState, menuType } = this.props;
    const values = this.getValues();
    if (!values.includes(value)) {
      const newFilterState = cloneDeep(filterStateFilters);
      newFilterState[menuType] = [...values, value];
      onUpdateFilterState(newFilterState);
    }
  };

  removeInfoFromFilter = (value) => {
    const { filterStateFilters, onUpdateFilterState, menuType } = this.props;
    const values = this.getValues();
    if (values.includes(value)) {
      const newFilterState = cloneDeep(filterStateFilters);
      newFilterState[menuType] = values.filter(v => v !== value);
      onUpdateFilterState(newFilterState);
    }
  };

  render() {
    const { filterItems, filterLabels, menuType } = this.props;
    const selectedValues = Immutable.List(this.getValues());
    const items = filterItems[menuType];
    const label = la(filterLabels[menuType]);
    return (
      <FilterSelectMenu
        selectedToTop={false}
        noSearch
        onItemSelect={this.addInfoToFilter}
        onItemUnselect={this.removeInfoFromFilter}
        selectedValues={selectedValues}
        items={items}
        label={label}
        name={menuType}
      />
    );
  }

}
