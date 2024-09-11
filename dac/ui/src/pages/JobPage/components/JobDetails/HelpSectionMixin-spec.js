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

import config from "dyn-load/utils/config";

import { HelpSection as HelpSectionBase } from "./HelpSection";
import HelpSectionMixin from "./HelpSectionMixin";

@HelpSectionMixin
class HelpSection extends HelpSectionBase {}

describe("HelpSectionMixin", () => {
  let minimalProps;
  let commonProps;
  const context = { loggedInUser: {} };
  beforeEach(() => {
    minimalProps = {
      jobId: "123-abc",
      callIfChatAllowedOrWarn: sinon.stub(),
      askGnarly: sinon.stub(),
      downloadFile: sinon.stub(),
      downloadViewState: Immutable.Map(),
    };
    commonProps = {
      ...minimalProps,
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<HelpSection {...minimalProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  describe("#getButtons", () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<HelpSection {...commonProps} />, { context });
      instance = wrapper.instance();
    });

    afterEach(() => {
      config.supportEmailTo = "";
    });

    it("should return email if config.supportEmailTo is defined", () => {
      config.supportEmailTo = "foo@bar.com";
      expect(instance.getButtons().keySeq().toJS()).to.be.eql(["email"]);
    });

    it("should not return email if config.supportEmailTo is undefined", () => {
      expect(instance.getButtons().keySeq().toJS()).to.be.eql([]);
    });

    it("should return download if support keys are enabled", () => {
      wrapper.setState({ profileDownloadEnabled: true });
      expect(instance.getButtons().keySeq().toJS()).to.be.eql(["download"]);
    });

    it("should not return download if support keys are disabled", () => {
      expect(instance.getButtons().keySeq().toJS()).to.be.eql([]);
    });
  });
});
