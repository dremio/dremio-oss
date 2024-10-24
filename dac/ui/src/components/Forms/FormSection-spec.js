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

import FormSection from "./FormSection";
import { VisibilityControl } from "./VisibilityControl";

describe("FormSection", () => {
  const minimalProps = {
    sectionConfig: {
      getSections() {
        return [];
      },

      getConfig() {
        return [];
      },

      getDirectElements() {
        return [];
      },
    },
  };

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<FormSection {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it("should render no children if visibilityControl is false", () => {
    const wrapper = shallow(
      <VisibilityControl
        visibilityControl={{
          config: "allowFileUploads",
          showCondition: false,
        }}
      >
        <div />
      </VisibilityControl>,
    );
    expect(wrapper.contains(<div />)).to.equal(false);
  });

  it("should render children if visibilityControl is true", () => {
    const wrapper = shallow(
      <VisibilityControl
        visibilityControl={{
          config: "allowFileUploads",
          showCondition: true,
        }}
      >
        <div />
      </VisibilityControl>,
    );
    expect(wrapper.contains(<div />)).to.equal(true);
  });
});
