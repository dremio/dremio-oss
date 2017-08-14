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

import Checkbox from 'components/Fields/Checkbox';

import EllipsisIcon from 'pages/ExplorePage/components/EllipsisIcon';

import { formDescription, body } from 'uiTheme/radium/typography';

import dataFormatUtils from 'utils/dataFormatUtils';

export default class SelectFrequentValuesOption extends Component {
  static propTypes = {
    option: PropTypes.shape({
      percent: PropTypes.any,
      value: PropTypes.any
    }),
    maxPercent: PropTypes.number,
    field: PropTypes.object,
    onShowMore: PropTypes.func,
    onCheck: PropTypes.func
  };

  constructor(props) {
    super(props);

    this.onEllipsisClick = this.onEllipsisClick.bind(this);
    this.onMouseEnter = this.onMouseEnter.bind(this);

    this.state = {
      cellStringBiggerThanCell: false
    };
  }

  shouldComponentUpdate(nextProps, nextState) {
    const { option, field } = this.props;
    return (option.value !== nextProps.option.value)
      || (field.value[option.value] !== nextProps.field.value[option.value])
      || (this.state.cellStringBiggerThanCell !== nextState.cellStringBiggerThanCell);
  }

  onEllipsisClick(anchor) {
    const { onShowMore, option } = this.props;

    onShowMore(option.value, anchor);
  }

  onMouseEnter(e) {
    const elem = $(e.target);
    const cellStringBiggerThanCell = elem.width() < elem.prop('scrollWidth');

    if (cellStringBiggerThanCell) {
      this.setState({
        cellStringBiggerThanCell
      });
    }
  }

  shouldShowEllipsis() {
    return this.state.cellStringBiggerThanCell;
  }

  renderLabelValue() {
    const { option } = this.props;
    const correctText = dataFormatUtils.formatValue(option.value);
    const correctTextStyle = option.value === undefined || option.value === null || option.value === ''
       ? {...styles.nullwrap}
       : {...styles.wrap};
    return (
      <span
        onMouseEnter={this.onMouseEnter}
        style={{...correctTextStyle, marginLeft: 10 }}
      >
        {correctText}
      </span>
    );
  }

  render() {
    const { option, field, maxPercent } = this.props;
    const isChecked = field.value[option.value];

    return (
      <tr>
        <td>
          <Checkbox
            data-qa={`checkbox${option.value}`}
            style={styles.checkbox}
            checked={isChecked}
            label={
              <div
                className={'container-cell' + (this.shouldShowEllipsis() ? ' explore-cell-overflow' : '')}
                style={styles.container}
              >
                <div className='cell-data'>
                  { this.renderLabelValue() }
                </div>
                { this.shouldShowEllipsis() && <EllipsisIcon
                  onClick={this.onEllipsisClick}
                  containerStyle={{ lineHeight: '18px' }}
                />}
              </div>
            }
            onChange={() => {
              this.props.onCheck(option.value);
            }}
          />
        </td>
        <td style={styles.progressWrap}>
          {/* todo: this is not a progress element, semantically. see <meter> */}
          <progress value={option.percent} max={maxPercent} style={styles.progress} />
        </td>
        <td>
          <div style={styles.percent}>
            {`${option.percent === 100 ? option.percent : option.percent.toPrecision(2)}%`}
          </div>
        </td>
      </tr>
    );
  }
}

const styles = {
  container: {
    display: 'flex',
    alignItems: 'flex-start',
    position: 'relative'
  },
  wrap: {
    overflow: 'hidden',
    whiteSpace: 'nowrap',
    textOverflow: 'ellipsis'
  },
  nullwrap: {
    color: '#aaa',
    fontStyle: 'italic',
    textAlign: 'center',
    width: '95%'
  },
  checkbox: {
    width: 200,
    marginTop: -2,
    ...body,
    overflow: 'hidden'
  },
  progressWrap: {
    width: '100%'
  },
  progress: {
    marginLeft: 5,
    width: '100%',
    position: 'relative',
    top: -4
  },
  percent: {
    ...formDescription,
    marginLeft: 5,
    textAlign: 'right'
  }
};
