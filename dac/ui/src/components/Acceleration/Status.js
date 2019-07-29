/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import Immutable from 'immutable';

import Art from 'components/Art';
import { getReflectionUiStatus } from 'utils/accelerationUtils';

export default class Status extends Component {
  static propTypes = {
    reflection: PropTypes.instanceOf(Immutable.Map).isRequired,
    style: PropTypes.object
  };

  render() {
    const { reflection, style } = this.props;
    const uiStatus = reflection.get('uiStatus') || getReflectionUiStatus(reflection);

    return <Art src={`${uiStatus.get('icon')}.svg`}
      style={{...style, height: 24}}
      alt={uiStatus.get('text')}
      className={uiStatus.get('className')}
      title />;
  }
}
