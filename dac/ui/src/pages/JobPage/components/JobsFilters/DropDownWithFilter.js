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
import Radium from 'radium';
import PureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Immutable  from 'immutable';

import FontIcon from 'components/Icon/FontIcon';

import './DropDownWithFilter.less';

// todo: loc

@Radium
@PureRender
export default class DropDownWithFilter extends Component {

  static propTypes = {
    availableOptions: PropTypes.instanceOf(Immutable.List).isRequired,
    filterPattern: PropTypes.string.isRequired,
    resetSelection: PropTypes.func.isRequired,
    onChange: PropTypes.func.isRequired,
    onCustomFilterChange: PropTypes.func.isRequired,
    options: PropTypes.instanceOf(Immutable.List).isRequired,
    selectedOptions: PropTypes.instanceOf(Immutable.List).isRequired
  };

  constructor(props) {
    super(props);
    this.onCustomFilterChange = this.onCustomFilterChange.bind(this);
  }

  onCustomFilterChange() {
    this.props.onCustomFilterChange(this.refs.customFilterInput);
  }

  render() {
    const { availableOptions, filterPattern, resetSelection, selectedOptions, onChange } = this.props;
    const selectOptions = selectedOptions.map((item, index) => {
      return (
        <div className='drop-option' key={`selected${index}`} style={styles.dropOption}>
          <input
            type='checkbox'
            onChange={onChange}
            className='drop-down-checkbox'
            value={item}
            checked/>
          <span>{item}</span>
        </div>);
    });
    const filteredOptions = filterPattern
      ? availableOptions.filter((item) => {
        return item.startsWith(filterPattern);
      })
      : availableOptions;
    const nonSelectedOptions = filteredOptions.map((item, index) => {
      return (
        <div className='drop-option' key={`nonSelected${index}`} style={styles.dropOption}>
          <input
            type='checkbox'
            className='drop-down-checkbox'
            onChange={onChange}
            value={item}
            checked={false}/>
          <span>{item}</span>
        </div>);
    });
    const allLabel = selectOptions.size !== 0
      ? <div onClick={resetSelection} className='all-label' style={styles.allLabels}>{la('All')}</div>
      : null;
    const filter = availableOptions.size !== 0
      ? <div className='custom-filter' style={styles.customFilter}>
        <input
          placeholder='Filter…'
          className='custom-filter-input'
          ref='customFilterInput'
          onChange={this.onCustomFilterChange}/>
        <FontIcon type='Search' theme={styles.iconSearch}/>
      </div>
      : null;
    return (
      <div className='drop-down-with-filter' style={[styles.base]}>
        <span>{allLabel}</span>
        <div className='scroll-box'>
          {selectOptions}
        </div>
        {filter}
        <div className='scroll-box'>
          {nonSelectedOptions}
        </div>
      </div>);
  }
}

const styles = {
  base: {
    background: '#fff',
    boxShadow: '0 0 5px #999',
    borderRadius: '2px',
    width:'200px',
    zIndex: '99'
  },
  customFilter: {
    position: 'relative',
    padding: '4px',
    background: 'rgb(241,241,241)'
  },
  iconSearch: {
    Container: {
      position: 'absolute',
      right: 8,
      top: 0,
      bottom: 0
    }
  },
  dropOption: {
    padding: '3px 6px'
  },
  allLabels: {
    borderBottom: '1px solid rgba(0,0,0,.1)',
    padding: '8px 6px',
    margin: '0'
  }
};
