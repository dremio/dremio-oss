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
import { shallow } from "enzyme";

import DurationBreakdown from "./DurationBreakdown";

describe("DurationBreakdown", () => {
  let minimalProps;
  let commonProps;
  const context = { loggedInUser: {} };
  beforeEach(() => {
    minimalProps = {
      pending: 2,
      engineStart: 0.2,
      queued: 5,
      starting: 0.1,
      running: 8,
    };
    commonProps = {
      ...minimalProps,
      metadataRetrival: 1,
      planning: 0.6,
      executionPlanning: 3,
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<DurationBreakdown {...minimalProps} />, {
      context,
    });
    expect(wrapper).to.have.length(1);
  });

  it("should render with commonProps props without exploding", () => {
    const wrapper = shallow(<DurationBreakdown {...commonProps} />, {
      context,
    });
    expect(wrapper).to.have.length(1);
  });
});
