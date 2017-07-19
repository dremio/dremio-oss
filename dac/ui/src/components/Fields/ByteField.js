/*
 * Copyright (C) 2017 Dremio Corporation
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

import MultiplierField from './MultiplierField';

const MULTIPLIERS = new Map([ // todo: loc
  ['B', 1024 ** 0],
  ['KB', 1024 ** 1],
  ['MB', 1024 ** 2],
  ['GB', 1024 ** 3],
  ['TB', 1024 ** 4]
]);

export default class ByteField extends Component {
  static propTypes = {}; // pass-thru

  render() {
    return <MultiplierField {...this.props} unitMultipliers={MULTIPLIERS} />;
  }
}
