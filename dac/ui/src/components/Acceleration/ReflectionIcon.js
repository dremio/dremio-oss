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
import Immutable from 'immutable';

import Art from 'components/Art';
import { formatMessage } from 'utils/locale';

export default class ReflectionIcon extends Component {
  static propTypes = {
    reflection: PropTypes.object.isRequired,
    style: PropTypes.object
  };

  render() {
    const { reflection, style } = this.props;

    let icon = '';
    let text = '';

    const type = Immutable.Map.isMap(reflection) ? reflection.get('type') : reflection.type;

    if (type === 'RAW') {
      text = 'Reflection.Raw';
      icon = 'RawMode';
    } else if (type === 'AGGREGATION') {
      text = 'Reflection.Aggregation';
      icon = 'Aggregate';
    } else if (type === 'EXTERNAL') {
      text = 'Reflection.External';
      icon = 'PhysicalDatasetGray';
    }

    return <Art src={`${icon}.svg`}
      style={{height: 24, ...style}}
      alt={formatMessage(text)}
      title />;
  }
}
