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

export default class Status extends Component {
  static propTypes = {
    reflection: PropTypes.instanceOf(Immutable.Map).isRequired,
    style: PropTypes.object
  };

  getTextWithFailureCount = (status, statusMessage) => {
    const msgId = (status.get('refresh') === 'MANUAL') ?
      'Reflection.StatusFailedNoReattempt' : 'Reflection.StatusFailedNonFinal';
    return formatMessage(msgId, {
      status: statusMessage,
      failCount: status.get('failureCount')
    });

  };

  render() {
    const { reflection, style } = this.props;
    const status = reflection.get('status');

    let icon = 'WarningSolid';
    let text = '';
    let className = '';

    const statusMessage = status && status.get('availability') === 'AVAILABLE' ?
      formatMessage('Reflection.StatusCanAccelerate') : formatMessage('Reflection.StatusCannotAccelerate');

    if (!reflection.get('enabled')) {
      icon = 'Disabled';
      text = formatMessage('Reflection.StatusDisabled');
    } else if (status.get('config') === 'INVALID') {
      icon = 'ErrorSolid';
      text = formatMessage('Reflection.StatusInvalidConfiguration', {status: statusMessage});
    } else if (status.get('refresh') === 'GIVEN_UP') {
      icon = 'ErrorSolid';
      text = formatMessage('Reflection.StatusFailedFinal', {status: statusMessage});
    } else if (status.get('availability') === 'INCOMPLETE') {
      icon = 'ErrorSolid';
      text = formatMessage('Reflection.StatusIncomplete', {status: statusMessage});
    } else if (status.get('availability') === 'EXPIRED') {
      icon = 'ErrorSolid';
      text = formatMessage('Reflection.StatusExpired', {status: statusMessage});
    } else if (status.get('refresh') === 'RUNNING') {
      if (status.get('availability') === 'AVAILABLE') {
        icon = 'OKSolid';
        text = formatMessage('Reflection.StatusRefreshing', {status: statusMessage});
      } else {
        icon = 'Loader';
        text = formatMessage('Reflection.StatusBuilding', {status: statusMessage});
        className = 'spinner';
      }
    } else if (status.get('availability') === 'AVAILABLE') {
      if (status.get('failureCount') > 0) {
        icon = 'WarningSolid';
        text = this.getTextWithFailureCount(status, statusMessage);
      } else if (status.get('refresh') === 'MANUAL') {
        icon = 'OKSolid';
        text = formatMessage('Reflection.StatusManual', {status: statusMessage});
      } else {
        icon = 'OKSolid';
        text = formatMessage('Reflection.StatusCanAccelerate');
      }
    } else if (status.get('failureCount') > 0) {
      icon = 'WarningSolid';
      text = this.getTextWithFailureCount(status, statusMessage);
    } else if (status.get('refresh') === 'SCHEDULED') {
      icon = 'Ellipsis';
      text = formatMessage('Reflection.StatusBuilding', {status: statusMessage});
    } else if (status.get('refresh') === 'MANUAL') {
      icon = 'WarningSolid';
      text = formatMessage('Reflection.StatusManual', {status: statusMessage});
    }

    return <Art src={`${icon}.svg`}
      style={{...style, height: 24}}
      alt={text}
      className={className}
      title />;
  }
}
