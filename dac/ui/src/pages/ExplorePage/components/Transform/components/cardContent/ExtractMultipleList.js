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

import { FieldWithError, TextField, Select } from 'components/Fields';

import { applyValidators, isRequired, isWholeNumber } from 'utils/validation';

import { formLabel, bodySmall } from 'uiTheme/radium/typography';

const DEFAULT_WIDTH = 200;

@Radium
export default class ExtractMultipleList extends Component {
  static getFields() {
    return [
      'multiple.startIndex.value',
      'multiple.startIndex.direction',
      'multiple.endIndex.value',
      'multiple.endIndex.direction'
    ];
  }

  static validate(values) {
    return applyValidators(values, [
      isRequired('multiple.startIndex.value', 'Start'),
      isWholeNumber('multiple.startIndex.value', 'Start'),
      isRequired('multiple.endIndex.value', 'End'),
      isWholeNumber('multiple.endIndex.value', 'End')
    ]);
  }

  static propTypes = {
    firstElementLabel: PropTypes.string,
    lastElementLabel: PropTypes.string,
    startIndex: PropTypes.object,
    endIndex: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.items = [
      {
        option: 'Start',
        label: 'Start'
      },
      {
        option: 'End',
        label: 'End'
      }
    ];
  }

  render() {
    const {startIndex, endIndex} = this.props;
    return (
      <div style={[styles.extract]}>
        <div style={[styles.largeItem, {marginTop: 0}]}>
          <div style={[styles.item]}>
            <span style={[styles.font, formLabel]}>
              {this.props.firstElementLabel}
            </span>
            <FieldWithError {...startIndex.value} errorPlacement='bottom'>
              <TextField
                {...startIndex.value}
                type='number'
                style={[styles.input]}/>
            </FieldWithError>
          </div>
          <div style={[styles.item]}>
            <span style={[styles.font, formLabel]}>
              Relative To
            </span>
            <Select
              items={this.items}
              {...startIndex.direction}
              style={styles.select}/>
          </div>
        </div>
        { endIndex
          ? <div style={[styles.largeItem]}>
            <div style={[styles.item]}>
              <span style={[styles.font, formLabel]}>
                {this.props.lastElementLabel}
              </span>
              <FieldWithError {...endIndex.value} errorPlacement='bottom'>
                <TextField
                  {...endIndex.value}
                  type='number'
                  style={[styles.input]}/>
              </FieldWithError>
            </div>
            <div style={[styles.item]}>
              <span style={[styles.font, formLabel]}>Relative To</span>
              <Select
                items={this.items}
                {...endIndex.direction}
                style={styles.select}/>
            </div>
          </div>
          : null}
      </div>
    );
  }
}

const styles = {
  item: {
    maxWidth: 200,
    marginLeft: 10,
    fontWeight: 400,
    marginTop: 5,
    display: 'flex',
    flexDirection: 'column',
    justifyContent: 'flex-start'
  },
  largeItem: {
    marginTop: 0,
    display: 'flex'
  },
  input: {
    width: 180,
    height: 28,
    fontSize: 13,
    border: '1px solid #ccc',
    borderRadius: 3,
    outline: 'none',
    marginRight: 0
  },
  select: {
    width: DEFAULT_WIDTH,
    height: 24,
    ...bodySmall
  },
  font: {
    margin: 0
  }
};
