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
import { humanSorter } from './sort';

describe('humanSorter', () => {
  it('should compare strings', () => {
    expect(humanSorter('a', 'b')).to.equal(-1);
    expect(humanSorter('a', 'a')).to.equal(0);
    expect(humanSorter('a', 'A')).to.equal(-1);
    expect(humanSorter('a', '-')).to.equal(1);
    expect(humanSorter('A', '-')).to.equal(1);
  });
  it('should compare string and number', () => {
    expect(humanSorter('a', 1)).to.equal(-1);
    expect(humanSorter('a', 62)).to.equal(-1);
    expect(humanSorter(5, '5')).to.equal(1);
  });
  it('should compare numbers', () => {
    expect(humanSorter(1, 1)).to.equal(0);
    expect(humanSorter(2, 3)).to.equal(-1);
    expect(humanSorter(3, 2)).to.equal(1);
  });
  it('should handle empty args', () => {
    expect(humanSorter(null, null)).to.equal(0);
    expect(humanSorter()).to.equal(0);
  });
  it('should handle non strings/numbers', () => {
    expect(humanSorter({a: 'a'}, {a: 'b'})).to.equal(0);
  });
});
