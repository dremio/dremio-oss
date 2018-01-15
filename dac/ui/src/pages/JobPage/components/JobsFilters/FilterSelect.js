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
import { Component } from 'react';
import Radium from 'radium';
import PureRender from 'pure-render-decorator';

import PropTypes from 'prop-types';

import FilterSelectView from './FilterSelectView';

@Radium
@PureRender
export default class FilterSelect extends Component {

  static propTypes = {
    id: PropTypes.string.isRequired,
    selectedOptions: PropTypes.object,
    options: PropTypes.object,
    activeFilter: PropTypes.object,
    defaultLabel: PropTypes.string.isRequired,
    setActiveFilter: PropTypes.func.isRequired,
    clearActiveFilter: PropTypes.func.isRequired
  };

  static defaultProps = {
    selectedOptions: new Immutable.List(),
    options: new Immutable.List()
  };

  constructor(props) {
    super(props);
    const {selectedOptions, options} = props;
    const availableOptions = this._getAvaibleOptions(options, selectedOptions);
    this.state = {
      selectedOptions,
      filterPattern: '',
      availableOptions
    };
    this.onChange = this.onChange.bind(this);
    this.onCustomFilterChange = this.onCustomFilterChange.bind(this);
    this.resetSelection = this.resetSelection.bind(this);
  }

  componentWillReceiveProps(nextProps) {
    const {selectedOptions, options} = nextProps;
    if (this.props.options !== options || this.props.selectedOptions !== selectedOptions) {
      const availableOptions = this._getAvaibleOptions(options, selectedOptions);
      this.setState({
        selectedOptions: selectedOptions.sort(),
        availableOptions
      });
    }
  }

  onChange(e) {
    if (e.target.checked) {
      this.setState({
        selectedOptions: this.state.selectedOptions.push(e.target.value).sort(),
        availableOptions: this.state.availableOptions.filter((item) => {
          return item !== e.target.value;
        })
      });
    } else {
      this.setState({
        selectedOptions: this.state.selectedOptions.filter((item) => {
          return item !== e.target.value;
        }),
        availableOptions: this.state.availableOptions.push(e.target.value).sort()
      });
    }
  }

  onCustomFilterChange(input) {
    clearTimeout(this.timer);
    this.timer = setTimeout( () => {
      this.setState({
        filterPattern: input.value
      });
    }, 300);
  }

  /**
   * return list of selected options from component state.
   * This function are used in JobsFilters.js.
   * @returns {*|selectedOptions|Immutable.List}
   */
  getSelectedOptions() {
    return this.state.selectedOptions;
  }

  resetSelection() {
    this.setState({
      selectedOptions: new Immutable.List(),
      availableOptions: this.props.options.sort()
    });
  }

  _getAvaibleOptions(options, selectedOptions) {
    return options.sort().filter((item) => {
      return selectedOptions.indexOf(item) === -1;
    });
  }

  render() {
    const {activeFilter, id, setActiveFilter, clearActiveFilter, defaultLabel, options} = this.props;
    const {selectedOptions, filterPattern, availableOptions} = this.state;
    const active = id === activeFilter;
    return (
      <FilterSelectView
        active={active}
        availableOptions={availableOptions}
        defaultLabel={defaultLabel}
        id={id}
        options={options}
        filterPattern={filterPattern}
        onChange={this.onChange}
        setActiveFilter={setActiveFilter}
        clearActiveFilter={clearActiveFilter}
        onCustomFilterChange={this.onCustomFilterChange}
        selectedOptions={selectedOptions}
        resetSelection={this.resetSelection}/>);
  }
}
