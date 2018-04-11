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
import SimpleButton from 'components/Buttons/SimpleButton';

import config from 'utils/config';

export default function(input) {
  Object.assign(input.prototype, { // eslint-disable-line no-restricted-properties
    getButtons() {
      const map = [
        config.supportEmailTo && ['email', <SimpleButton
          buttonStyle='secondary'
          children={la('Email Help')}
          onClick={this.handleEmail}/>],
        ['download', <SimpleButton
          buttonStyle='secondary'
          children={la('Download Profile')}
          submitting={this.props.downloadViewState.get('isInProgress')}
          onClick={this.handleDownload}/>]
      ];

      return new Immutable.OrderedMap(map);
    }
  });
}
