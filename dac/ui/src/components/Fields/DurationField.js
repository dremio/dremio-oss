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

import MultiplierField from './MultiplierField';

const MULTIPLIERS = new Map([ // todo: loc, proper pluralization
  ['Millisecond(s)', 1],
  ['Second(s)', 1000],
  ['Minute(s)', 60 * 1000],
  ['Hour(s)', 60 * 60 * 1000],
  ['Day(s)', 24 * 60 * 60 * 1000],
  ['Week(s)', 7 * 24 * 60 * 60 * 1000]
]);

export default class DurationField extends Component {
  static propTypes = {}; // pass-thru

  render() {
    return <MultiplierField {...this.props} unitMultipliers={MULTIPLIERS} />;
  }
}
