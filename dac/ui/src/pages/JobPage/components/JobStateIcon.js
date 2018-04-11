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
import { PureComponent } from 'react';
import PropTypes from 'prop-types';
import { injectIntl } from 'react-intl';

import Art from 'components/Art';

@injectIntl
export default class JobStateIcon extends PureComponent {
  static propTypes = {
    state: PropTypes.string.isRequired,
    intl: PropTypes.object.isRequired,
    style: PropTypes.object
  };

  render() {
    const state = this.props.state;

    let src = 'Ellipsis';
    let className = '';

    if (icons[state]) {
      if (typeof icons[state] === 'string') {
        src = icons[state];
      } else {
        src = icons[state].src;
        className = icons[state].className;
      }
    }

    return <Art
      src={`${src}.svg`}
      alt={this.props.intl.formatMessage({id: 'Job.State.' + state})}
      style={{height: 24, ...this.props.style}}
      className={className}
    />;
  }
}

const icons = {
  'NOT_SUBMITTED': 'Ellipsis',
  'STARTING': 'Ellipsis',
  'RUNNING': { src: 'Loader', className: 'spinner' },
  'COMPLETED': 'OKSolid',
  'CANCELED': 'Canceled',
  'FAILED': 'ErrorSolid',
  'CANCELLATION_REQUESTED': 'CanceledGray',
  'ENQUEUED': 'Ellipsis'
};
