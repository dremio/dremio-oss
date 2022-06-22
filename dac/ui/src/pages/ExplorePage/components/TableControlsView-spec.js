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

import TableControlsView from "./TableControlsView";

describe("TableControlsView", () => {
  let commonProps;
  let minimalProps;
  let context;
  beforeEach(() => {
    minimalProps = {
      closeDropdown: sinon.stub(),
      toogleDropdown: sinon.stub(),
      groupBy: sinon.stub(),
      handleRequestClose: sinon.stub(),
      columnNames: Immutable.fromJS([]),
      join: sinon.stub(),
      dropdownState: false,
      exploreViewState: Immutable.Map(),
      dataset: Immutable.Map(),
      tableColumns: Immutable.List(),
    };
    commonProps = {
      ...minimalProps,
      approximate: true,
    };
    context = {
      location: {},
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<TableControlsView {...minimalProps} />, {
      context,
    });
    expect(wrapper).to.have.length(1);
    expect(wrapper.find("div[data-qa='columnFilterStats']")).to.have.length(1);
    expect(wrapper.find("span[data-qa='columnFilterCount']")).to.have.length(0);
  });

  it("should render with common props without exploding", () => {
    const wrapper = shallow(<TableControlsView {...commonProps} />, {
      context,
    });
    expect(wrapper).to.have.length(1);
    expect(wrapper.find("div[data-qa='columnFilterStats']")).to.have.length(1);
    expect(wrapper.find("span[data-qa='columnFilterCount']")).to.have.length(0);
  });
});
