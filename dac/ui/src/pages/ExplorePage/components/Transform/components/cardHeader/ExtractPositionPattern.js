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
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import Radio from 'components/Fields/Radio';

import { fixedWidthBold } from 'uiTheme/radium/typography';

@pureRender
@Radium
export default class ExtractPositionPattern extends Component {

  static propTypes = {
    id: PropTypes.string.isRequired,
    handleTypeChange: PropTypes.func.isRequired,
    type: PropTypes.string.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    return (
      <div style={[styles.extract]}>
        <span style={[fixedWidthBold]}>
          Extract By
        </span>
        <Radio
          onChange={this.props.handleTypeChange.bind(this, 'position')}
          radioValue='position'
          value={this.props.type.indexOf('pattern') !== -1 ? 'pattern' : 'position'}
          label='Position'
          style={{...fixedWidthBold, ...styles.radio}}/>
        <Radio
          onChange={this.props.handleTypeChange.bind(this, 'pattern')}
          radioValue='position'
          value={this.props.type.indexOf('pattern') === -1 ? 'pattern' : 'position'}
          label='Pattern'
          style={{...fixedWidthBold, ...styles.radio}}/>
      </div>
    );
  }
}

const styles = {
  position: {
    marginLeft: 15,
    width: 15,
    height: 15,
    position: 'relative',
    top: 3
  },
  radio: {
    marginTop: 10,
    display: 'inline-block',
    width: 75
  },
  extract: {
    marginLeft: 15
  },
  text: {
    marginLeft: 5,
    fontWeight: 400
  }
};
