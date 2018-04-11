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
import Immutable from 'immutable';

import { handleAutoPeek } from './autoPeek';

describe('autoPeek saga', () => {
  describe('handleAutoPeek()', () => {
    let gen;
    let next;
    beforeEach(() => {
      gen = handleAutoPeek({meta: {peekId: 'fooid'}});
    });

    it('should navigate on success', () => {
      next = gen.next();
      next = gen.next(); // select(getLocation)
      expect(typeof next.value.RACE.success).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;
      const success = {payload: Immutable.Map({result: 'foo'})};
      next = gen.next({success});
      expect(next.value.PUT).to.not.be.undefined;
      next = gen.next();
      expect(next.done).to.be.true;
    });

    it('should not navigate on locationChange', () => {
      next = gen.next();
      next = gen.next(); // select(getLocation)
      expect(typeof next.value.RACE.success).to.not.be.undefined;
      expect(typeof next.value.RACE.locationChange).to.not.be.undefined;
      const locationChange = {foo: true};
      next = gen.next({locationChange});
      expect(next.done).to.be.true;
    });
  });
});
