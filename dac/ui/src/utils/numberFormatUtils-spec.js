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
import NumberFormatUtils from './numberFormatUtils';
import { MEMORY_UNITS } from './numberFormatUtils';

describe('NumberFormatUtils', () => {

  it('should have memory units map', () => {
    expect(MEMORY_UNITS).to.be.defined;
    expect(MEMORY_UNITS.size).to.equal(4);
    expect(MEMORY_UNITS.get('KB')).to.equal(1024);
    expect(MEMORY_UNITS.get('GB')).to.equal(1024 * 1024 * 1024);
  });

  it('should format memory value', () => {
    expect(NumberFormatUtils.makeMemoryValueString(0)).to.equal('0 B');
    expect(NumberFormatUtils.makeMemoryValueString(1024)).to.equal('1 kB');
    expect(NumberFormatUtils.makeMemoryValueString(1024 * 1024 * 1024)).to.equal('1 GB');
    expect(NumberFormatUtils.makeMemoryValueString(3 * 1024 * 1024 * 1024)).to.equal('3 GB');
    expect(NumberFormatUtils.makeMemoryValueString(8 * 1024 * 1024 * 1024)).to.equal('8 GB');
    expect(NumberFormatUtils.makeMemoryValueString(8000 * 1024 * 1024)).to.equal('7.81 GB'); //long decimal
    expect(NumberFormatUtils.makeMemoryValueString(1536 * 1024 * 1024)).to.equal('1.5 GB'); //short decimal
    expect(NumberFormatUtils.makeMemoryValueString(1.5 * 1024 * 1024)).to.equal('1.5 MB'); //short decimal
    expect(NumberFormatUtils.makeMemoryValueString(1.53 * 1024 * 1024)).to.equal('1.53 MB'); //short decimal
    expect(NumberFormatUtils.makeMemoryValueString(1.5209874 * 1024 * 1024)).to.equal('1.52 MB'); //long decimal
    expect(NumberFormatUtils.makeMemoryValueString(1520.9874 * 1024 * 1024)).to.equal('1.49 GB'); //long decimal
  });

});
