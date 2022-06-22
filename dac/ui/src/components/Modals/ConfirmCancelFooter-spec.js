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

import ConfirmCancelFooter from "./ConfirmCancelFooter";

describe("ConfirmCancelFooter", () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      confirm: sinon.spy(),
      cancel: sinon.spy(),
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<ConfirmCancelFooter {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  //DX-28985
  xit("should render two SimpleButtons", () => {
    const wrapper = shallow(<ConfirmCancelFooter {...commonProps} />);
    expect(wrapper.find("SimpleButton")).to.have.length(2);
  });

  //DX-28985
  xit("should hide the cancel button when hideCancel is set", () => {
    const wrapper = shallow(
      <ConfirmCancelFooter {...commonProps} hideCancel />
    );
    expect(wrapper.find("SimpleButton")).to.have.length(1);
    expect(wrapper.find("SimpleButton").first().props()["data-qa"]).to.eql(
      "confirm"
    );
  });

  //DX-28985
  xit("should hide the cancel button when no cancel function", () => {
    const wrapper = shallow(
      <ConfirmCancelFooter {...commonProps} cancel={null} />
    );
    expect(wrapper.find("SimpleButton")).to.have.length(1);
    expect(wrapper.find("SimpleButton").first().props()["data-qa"]).to.eql(
      "confirm"
    );
  });

  //DX-28985
  xit('should set type="submit" on confirm button when submitForm is set', () => {
    const wrapper = shallow(<ConfirmCancelFooter {...commonProps} />);
    expect(wrapper.find("SimpleButton").at(1).props().type).to.be.undefined;
    wrapper.setProps({ submitForm: true });
    expect(wrapper.find("SimpleButton").at(1).props().type).to.be.eql("submit");
  });

  //DX-28985
  xit("should set submitting prop on confirm button", () => {
    const wrapper = shallow(<ConfirmCancelFooter {...commonProps} />);
    expect(wrapper.find("SimpleButton").at(1).props().submitting).to.be.false;
    wrapper.setProps({ submitting: true });
    expect(wrapper.find("SimpleButton").at(1).props().submitting).to.be.true;
  });

  //DX-28985
  xit("should disable submit button if canSubmit is false", () => {
    const wrapper = shallow(<ConfirmCancelFooter {...commonProps} />);
    expect(wrapper.find("SimpleButton").at(1).props().disabled).to.be.false;
    wrapper.setProps({ canSubmit: false });
    expect(wrapper.find("SimpleButton").at(1).props().disabled).to.be.true;
  });

  //DX-28985
  xit("should disable cancel button if canCancel is false", () => {
    const wrapper = shallow(<ConfirmCancelFooter {...commonProps} />);
    expect(wrapper.find("SimpleButton").at(0).props().disabled).to.be.false;
    wrapper.setProps({ canCancel: false });
    expect(wrapper.find("SimpleButton").at(0).props().disabled).to.be.true;
  });
});
