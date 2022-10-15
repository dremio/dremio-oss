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
import EllipsedText from "./EllipsedText";

function getProps(wrapper) {
  return wrapper.dive().props();
}

describe("EllipsedText", () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      text: "foo",
      className: "another",
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<EllipsedText {...minimalProps} />);
    expect(wrapper).to.have.length(1);
    expect(getProps(wrapper).className.trim()).to.be.equal("EllipsedText");
  });

  it("should render with common props without exploding", () => {
    const wrapper = shallow(
      <EllipsedText {...commonProps}>
        <span>bar</span>
      </EllipsedText>
    );
    expect(wrapper).to.have.length(1);
    expect(getProps(wrapper).className).to.be.equal("EllipsedText another");
    expect(wrapper.dive().text()).to.be.equal("bar");
  });

  it("should render with just text", () => {
    const wrapper = shallow(<EllipsedText {...commonProps} />);
    expect(wrapper).to.have.length(1);
    expect(wrapper.dive().text()).to.be.equal("foo");
  });
});
