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
import Immutable from 'immutable';
import { getAnalyzeToolsConfig } from './config';

describe('getAnalyzeToolsConfig', () => {
  let settings;
  let config;

  beforeEach(() => {
    settings = Immutable.fromJS({
      'client.tools.tableau': { value: true },
      'client.tools.powerbi': { value: true },
      'client.tools.qlik': { value: false }
    });
    config = {
      analyzeTools: {
        tableau: { enabled: false },
        powerbi: { enabled: true },
        qlik: { enabled: false }
      }
    };
  });


  it('should use settings if provided', () => {
    const result = getAnalyzeToolsConfig(settings, config);
    expect(result.tableau.enabled).to.equal(true);
  });

  it('should use config if settings is not defined', () => {
    const result = getAnalyzeToolsConfig(null, config);
    expect(result.tableau.enabled).to.equal(false);
  });

  it('should use config if settings is empty', () => {
    const result = getAnalyzeToolsConfig(Immutable.fromJS({}), config);
    expect(result.tableau.enabled).to.equal(false);
  });

});
