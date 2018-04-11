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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import { fixedWidthBold } from 'uiTheme/radium/typography';

//TODO andrey, here we not sure what data we ger, need to improve architecture
@pureRender
@Radium
export default class ContentHeader extends Component {

  static propTypes = {
    type: PropTypes.string.isRequired,
    data: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { data } = this.props;
    return (
      <div style={[styles.extract]}>
        <span style={[fixedWidthBold]}>On:&nbsp;</span>
        <span style={[fixedWidthBold]}>{data.get('description')}</span>
      </div>
    );
  }
}

const styles = {
  extract: {
    marginLeft: 10,
    marginTop: 10
  }
};
