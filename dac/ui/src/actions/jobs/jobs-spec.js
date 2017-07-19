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
import { expect } from 'chai';

import * as Actions from './jobs.js';

describe('graph actions', () => {
  describe('test constants', () => {
    it('verify existence of LOAD_EXPLORE_GRAPH', () => {
      expect(Actions.FILTER_JOBS_REQUEST).to.not.be.undefined;
      expect(Actions.FILTER_JOBS_SUCCESS).to.not.be.undefined;
      expect(Actions.FILTER_JOBS_FAILURE).to.not.be.undefined;
    });
  });
});
