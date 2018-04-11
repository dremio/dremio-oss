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

import { LINE_CENTER_CENTER } from 'uiTheme/radium/flexStyle.js';
import { formLabel } from 'uiTheme/radium/typography';

import TextField from 'components/Fields/TextField';

@pureRender
@Radium
export default class ExtractMap extends Component {

  static propTypes = {
    path: PropTypes.object.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const {path} = this.props;
    return (
      <div style={styles.base}>
        <TextField {...path} style={styles.input}/>
      </div>
    );
  }
}

const styles = {
  base: {
    width: '100%',
    marginTop: 10,
    ...LINE_CENTER_CENTER
  },
  input: {
    width: '100%',
    margin: '0 10px 0 10px',
    ...formLabel
  }
};
