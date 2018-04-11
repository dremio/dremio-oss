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

import { FieldWithError, TextField/*, Select */} from 'components/Fields';

import { applyValidators, isRequired, isWholeNumber } from 'utils/validation';

import { formLabel, bodySmall } from 'uiTheme/radium/typography';

const DEFAULT_WIDTH = 200;

@Radium
export default class ExtractSingleList extends Component {
  static getFields() {
    return [
      'single.startIndex.value',
      'single.startIndex.direction'
    ];
  }

  static validate(values) {
    return applyValidators(values, [
      isRequired('single.startIndex.value', 'Start'),
      isWholeNumber('single.startIndex.value', 'Start')
    ]);
  }

  static propTypes = {
    firstElementLabel: PropTypes.string,
    startIndex: PropTypes.object
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
    const {startIndex} = this.props;
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
          {/* <div style={[styles.item]}> // put it back when DX-7090 is complete
            <span style={[styles.font, formLabel]}>
              Relative To
            </span>
            <Select
              items={this.items}
              {...startIndex.direction}
              style={styles.select}/>
          </div> */}
        </div>
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
    marginRight: 0,
    outline: 'none'
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
