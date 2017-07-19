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
import {Component, PropTypes} from 'react';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import Immutable  from 'immutable';

import DropDownWithFilter from './DropDownWithFilter';
import SelectView from './SelectView';
import StateDropDown from './StateDropDown';

@PureRender
@Radium
export default class FilterSelectView extends Component {

  static propTypes = {
    activeFilter: PropTypes.string,
    availableOptions: PropTypes.instanceOf(Immutable.List).isRequired,
    defaultLabel: PropTypes.string.isRequired,
    active: PropTypes.bool.isRequired,
    id: PropTypes.string.isRequired,
    filterPattern: PropTypes.string.isRequired,
    options: PropTypes.instanceOf(Immutable.List),
    onChange: PropTypes.func.isRequired,
    onCustomFilterChange: PropTypes.func.isRequired,
    setActiveFilter: PropTypes.func.isRequired,
    clearActiveFilter: PropTypes.func.isRequired,
    selectedOptions: PropTypes.instanceOf(Immutable.List).isRequired,
    resetSelection: PropTypes.func.isRequired
  };

  getLabel(value) {
    const {id, options} = this.props;
    if (id === 'stateFilter') {
      const neededOption = options.find((item) => {
        return item.get('value') === value;
      });
      if (neededOption) {
        return neededOption.get('label');
      }
    }
    return value;
  }

  getDropDown(id) {
    const {options, filterPattern, selectedOptions, availableOptions,
      resetSelection, onChange, onCustomFilterChange} = this.props;
    const dropDownHash = {
      'stateFilter': (
        <StateDropDown
          resetSelection={resetSelection}
          options={options}
          onChange={onChange}
          selectedOptions={selectedOptions} />
      ),
      'more': (
        <StateDropDown
          resetSelection={resetSelection}
          options={options}
          onChange={onChange}
          selectedOptions={selectedOptions} />
      ),
      'userFilter': (
        <DropDownWithFilter
          availableOptions={availableOptions}
          filterPattern={filterPattern}
          resetSelection={resetSelection}
          onChange={onChange}
          onCustomFilterChange={onCustomFilterChange}
          options={options}
          selectedOptions={selectedOptions} />
      ),
      'spaceFilter': (
        <DropDownWithFilter
          availableOptions={availableOptions}
          filterPattern={filterPattern}
          resetSelection={resetSelection}
          onChange={onChange}
          onCustomFilterChange={onCustomFilterChange}
          options={options}
          selectedOptions={selectedOptions} />
      )
    };
    return dropDownHash[id];
  }

  getFilterLabel() {
    const {defaultLabel, selectedOptions} = this.props;
    const filterLabels = [`${defaultLabel}: All`];
    const len = selectedOptions.size;
    for (let i = 0; i < len; i++) {
      if (i === 2) {
        filterLabels.push('...');
        break;
      }
      filterLabels[i] = this.getLabel(selectedOptions.get(i));
    }
    return filterLabels.join(', ');
  }

  render() {
    const {active, id, setActiveFilter, clearActiveFilter} = this.props;
    const label = this.getFilterLabel();
    return (
      <SelectView
        isActive={active}
        getDropDown={this.getDropDown.bind(this, id)}
        label={label}
        id={id}
        setActiveFilter={setActiveFilter}
        clearActiveFilter={clearActiveFilter}/>
    );
  }
}
