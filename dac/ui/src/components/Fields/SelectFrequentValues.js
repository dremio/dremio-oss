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

import PropTypes from 'prop-types';

import ExploreCellLargeOverlay from 'pages/ExplorePage/components/ExploreTable/ExploreCellLargeOverlay';

import SelectFrequentValuesOption from './SelectFrequentValuesOption';

@Radium
export default class SelectFrequentValues extends Component {
  static propTypes = {
    style: PropTypes.object,
    field: PropTypes.object.isRequired,
    options: PropTypes.arrayOf(
      PropTypes.shape({
        percent: PropTypes.any,
        value: PropTypes.any
      })
    )
  };

  static defaultProps = {
    options: []
  };

  state = {
    activeCell: false
  }

  handleCheck = (value, isChecked) => {
    const { field } = this.props;
    const newSet = new Set(field.value);
    if (isChecked) {
      newSet.add(value);
    } else {
      newSet.delete(value);
    }
    field.onChange([...newSet]);
  }

  showMore = (cellValue, anchor) => {
    this.setState({
      activeCell: { cellValue, anchor }
    });
  }

  hideMore = () => {
    this.setState({
      activeCell: false
    });
  }

  renderExploreCellLargeOverlay() {
    return this.state.activeCell ? <ExploreCellLargeOverlay {...this.state.activeCell} hide={this.hideMore} /> : null;
  }

  renderOption(option, maxPercent, valueSet) {
    return (
      <SelectFrequentValuesOption
        maxPercent={maxPercent}
        onShowMore={this.showMore}
        checked={valueSet.has(option.value)}
        option={option}
        onCheck={this.handleCheck}
        key={JSON.stringify(option.value) || 'undefined'} // JSON.stringify for null vs "null"
      />
    );
  }

  render() {
    const { options, style, field } = this.props;
    const maxPercent = options.reduce((prev, cur) => Math.max(prev, cur.percent), 0);

    const valueSet = new Set(field.value);
    return (
      <div className='field' style={{...style}}>
        <table>
          <tbody>
            {options && options.map((option) => this.renderOption(option, maxPercent, valueSet))}
          </tbody>
        </table>
        {this.renderExploreCellLargeOverlay()}
      </div>
    );
  }
}
