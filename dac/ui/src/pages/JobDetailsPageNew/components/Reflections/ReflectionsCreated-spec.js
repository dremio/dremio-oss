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
import Immutable from "immutable";
import ReflectionCreated from "./ReflectionsCreated";

const reflections = Immutable.fromJS([
  {
    reflectionName: "Raw Reflection",
    reflectionCreated: "1626236060790",
    reflectionPath: "transaction_analysis.base_views.scrubbed_transactions",
  },
  {
    reflectionName: "Reflection.TransactionsRaw",
    reflectionCreated: "1626237060360",
    reflectionPath: "transaction_analysis.base_views.scrubbed_transactions",
  },
]);

describe("reflectionCreated", () => {
  const minimalProps = {
    reflections: new Immutable.List(),
    location: {},
    jobStartTime: 1631593860360,
  };

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<ReflectionCreated {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  it("renders child correctly", () => {
    minimalProps.reflections = reflections;
    const wrapper = shallow(<ReflectionCreated {...minimalProps} />);
    expect(wrapper.find('[data-qa="reflectionsTestCase"]').length).equal(
      reflections.size,
    );
  });
});
