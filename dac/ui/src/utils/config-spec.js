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
import { getAnalyzeToolsConfig } from "./config";

describe("getAnalyzeToolsConfig", () => {
  let config;

  beforeEach(() => {
    config = {
      analyzeTools: {
        tableau: { enabled: false },
        powerbi: { enabled: true },
        qlik: { enabled: false },
      },
    };
  });

  it("should fetch analyze tools settings from config", () => {
    const result = getAnalyzeToolsConfig(config);
    expect(result.tableau.enabled).to.equal(false);
    expect(result.powerbi.enabled).to.equal(true);
    expect(result.qlik.enabled).to.equal(false);
  });
});
