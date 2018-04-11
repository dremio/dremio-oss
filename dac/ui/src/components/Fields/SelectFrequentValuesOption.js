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

import Checkbox from 'components/Fields/Checkbox';
import EllipsedText from 'components/EllipsedText';
import Meter from 'components/Meter';

import { formDescription } from 'uiTheme/radium/typography';

import dataFormatUtils from 'utils/dataFormatUtils';

export default class SelectFrequentValuesOption extends Component {
  static propTypes = {
    option: PropTypes.shape({
      percent: PropTypes.number,
      value: PropTypes.any
    }).isRequired,
    maxPercent: PropTypes.number.isRequired,
    checked: PropTypes.bool.isRequired,
    onShowMore: PropTypes.func, // todo: what's this supposed to do?
    onCheck: PropTypes.func.isRequired
  };

  shouldComponentUpdate(nextProps) {
    const { option, checked } = this.props;
    return option.value !== nextProps.option.value || checked !== nextProps.checked;
  }

  renderLabelValue() {
    const { option } = this.props;
    const correctText = dataFormatUtils.formatValue(option.value);
    // note: some APIs don't express null correctly (instead they drop the field)
    const correctTextStyle = option.value === undefined || option.value === null || option.value === ''
       ? styles.emptyTextWrap
       : {};
    return <EllipsedText text={correctText} style={{ ...correctTextStyle, marginLeft: 10 }} />;
  }

  render() {
    const { option, maxPercent, checked } = this.props;

    return (
      <tr className='field'>
        <td>
          <Checkbox
            data-qa={`checkbox${option.value}`}
            style={styles.checkbox}
            checked={checked}
            label={this.renderLabelValue()}
            onChange={(event) => {
              this.props.onCheck(option.value, event.target.checked);
            }}
          />
        </td>
        <td style={styles.progressWrap}>
          <Meter value={option.percent} max={maxPercent}/>
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
  emptyTextWrap: {
    color: '#aaa',
    fontStyle: 'italic',
    width: '95%'
  },
  checkbox: {
    width: 200,
    marginTop: -2,
    overflow: 'hidden'
  },
  progressWrap: {
    width: '100%',
    paddingLeft: 10
  },
  percent: {
    ...formDescription,
    marginLeft: 10,
    textAlign: 'right'
  }
};
