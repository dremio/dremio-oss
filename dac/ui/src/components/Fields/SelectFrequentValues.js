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
import { Component, PropTypes } from 'react';
import Radium from 'radium';

import ExploreCellLargeOverlay from 'pages/ExplorePage/components/ExploreTable/ExploreCellLargeOverlay';

import SelectFrequentValuesOption from './SelectFrequentValuesOption';

const MAX_SUGGESTIONS = 100;

@Radium
export default class SelectFrequentValues extends Component {
  static propTypes = {
    style: PropTypes.object,
    field: PropTypes.object,
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

  constructor(props) {
    super(props);

    this.showMore = this.showMore.bind(this);
    this.hideMore = this.hideMore.bind(this);
    this.onCheck = this.onCheck.bind(this);

    this.state = {
      activeCell: false
    };
  }

  onCheck(value) {
    const { field } = this.props;
    const isChecked = field.value[value];
    field.onChange({
      ...field.value,
      [value]: !isChecked
    });
  }

  showMore(cellValue, anchor) {
    this.setState({
      activeCell: { cellValue, anchor }
    });
  }

  hideMore() {
    this.setState({
      activeCell: false
    });
  }

  renderExploreCellLargeOverlay() {
    return this.state.activeCell
      ? (
        <ExploreCellLargeOverlay
          {...this.state.activeCell}
          hide={this.hideMore}
      />
    )
      : null;
  }

  renderOption(option, maxPercent) {
    const { field } = this.props;
    return (
      <SelectFrequentValuesOption
        maxPercent={maxPercent}
        onShowMore={this.showMore}
        field={field}
        option={option}
        onCheck={this.onCheck}
        key={option.value}
      />
    );
  }

  render() {
    const { options, style } = this.props;
    const maxPercent = options.reduce((prev, cur) => Math.max(prev, cur.percent), 0);
    return (
      <div style={{...style}}>
        <table>
          <tbody>
            {options && options
              .slice(0, MAX_SUGGESTIONS)
              .map((option) => this.renderOption(option, maxPercent))}
          </tbody>
        </table>
        {this.renderExploreCellLargeOverlay()}
      </div>
    );
  }
}
