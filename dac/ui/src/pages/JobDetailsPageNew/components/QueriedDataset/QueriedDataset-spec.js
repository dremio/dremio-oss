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
import QueriedDataset from "./QueriedDataset";

describe("QueriedDataset", () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      queriedDataSet: new Immutable.List(),
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<QueriedDataset {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  it("renders child correctly", () => {
    const queriedDataSet = Immutable.List([
      Immutable.fromJS({
        icon: "VirtualDataset.svg",
        datasetName: "Scrubbed_Transactions",
        datasetPath: "transaction_analysis.base_views.scrubbed",
      }),
      Immutable.fromJS({
        icon: "VirtualDataset.svg",
        datasetName: "Customers",
        datasetPath: "transaction_analysis.base_views.customer",
      }),
      Immutable.fromJS({
        icon: "PhysicalDataset.svg",
        datasetName: "Account_Types",
        datasetPath: "lake_source.lookup_tables.account_types",
      }),
    ]);
    const wrapper = shallow(<QueriedDataset queriedDataSet={queriedDataSet} />);
    expect(wrapper.find(".queriedDataset-dataWrapper__wrapper").length).equal(
      queriedDataSet.size
    );
  });
});
