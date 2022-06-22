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
import Immutable from "immutable";
import { getFilteredEngines } from "./EngineFilterHelper";

describe("EngineFilterHelper", () => {
  const emptyEngines = Immutable.fromJS([]);
  const e1 = { currentState: "RUNNING", dynamicConfig: { containerCount: 4 } };
  const e2 = { currentState: "STOPPED", dynamicConfig: { containerCount: 2 } };
  const engines = Immutable.fromJS([e1, e2]);

  it("should handle no filters", () => {
    expect(getFilteredEngines(emptyEngines, {})).to.eql(emptyEngines);
    expect(getFilteredEngines(engines)).to.eql(engines);
    expect(getFilteredEngines(engines, {})).to.eql(engines);
  });

  it("should filter by status", () => {
    const result = getFilteredEngines(engines, { st: ["RUNNING"] });
    expect(result.size).to.eql(1);
    expect(result.get(0).toJS()).to.eql(e1);
  });
  it("should filter by size", () => {
    const result = getFilteredEngines(engines, { sz: ["SMALL"] });
    expect(result.size).to.eql(1);
    expect(result.get(0).toJS()).to.eql(e2);
  });
});
