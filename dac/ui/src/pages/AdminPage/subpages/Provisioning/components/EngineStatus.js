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
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import {CLUSTER_STATE, CLUSTER_STATE_ICON} from '@app/constants/provisioningPage/provisioningConstants';
import Art from '@app/components/Art';

const unknownStateIcon = CLUSTER_STATE_ICON[CLUSTER_STATE.unknown];
const spinnerIcon = { src: 'Loader.svg', text: 'working...', className: 'spinner' };

export function EngineStatusIcon(props) {
  const {status, isInProgress, style} = props;
  let icon = (isInProgress) ? spinnerIcon : CLUSTER_STATE_ICON[status];
  if (!icon) {
    icon = {...unknownStateIcon, text: `${unknownStateIcon.text}, currentStatus: ${status}`};
  }
  return (
    <Art src={icon.src}
      style={{height: 24, width: 24, ...style}}
      alt={icon.text}
      className={icon.className}
      title
    />
  );
}
EngineStatusIcon.propTypes = {
  status: PropTypes.string,
  isInProgress: PropTypes.bool,
  style: PropTypes.object
};

export default function EngineStatus(props) {
  const {engine, style, viewState} = props;
  if (!engine) return null;

  const isInProgress = viewState && viewState.get('isInProgress')
    && viewState.get('entityId') === engine.get('id');
  const status = engine.get('currentState');
  return <EngineStatusIcon status={status} isInProgress={isInProgress} style={style}/>;
}

EngineStatus.propTypes = {
  engine: PropTypes.instanceOf(Immutable.Map),
  style: PropTypes.object,
  viewState: PropTypes.instanceOf(Immutable.Map)
};
