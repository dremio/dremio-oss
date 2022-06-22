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

import { JobDetailsWrapper } from "./JobDetailsWrapper";

describe("JobDetailsWrapper", () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      loadJobDetails: sinon.stub().returns(Promise.resolve()),
      cancelJob: sinon.spy(),
      downloadFile: sinon.spy(),
      askGnarly: sinon.spy(),
      viewState: Immutable.Map(),
    };
    commonProps = {
      ...minimalProps,
      jobId: "123",
      jobDetails: Immutable.fromJS({ state: "COMPLETED", attemptDetails: [] }),
      viewStateWrapper: Immutable.Map(),
      location: {},
      updateViewState: sinon.stub(),
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<JobDetailsWrapper {...minimalProps} />);
    expect(wrapper).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  it("should render JobDetails", () => {
    const wrapper = shallow(<JobDetailsWrapper {...commonProps} />);
    expect(wrapper.find("JobDetails")).to.have.length(1);
    wrapper.instance().componentWillUnmount();
  });

  describe("componentWillReceiveProps", () => {
    it("should call loadJobDetails when jobId changes", () => {
      const wrapper = shallow(<JobDetailsWrapper {...commonProps} />);
      expect(commonProps.loadJobDetails).to.be.calledOnce;

      wrapper.setProps({ jobId: commonProps.jobId });
      expect(commonProps.loadJobDetails).to.be.calledOnce;
      wrapper.setProps({ jobId: "456" });
      expect(commonProps.loadJobDetails).to.be.calledTwice;
      wrapper.instance().componentWillUnmount();
    });
  });

  describe("load", () => {
    it("should handle 404 responses and call updateViewState", (done) => {
      const props = {
        ...commonProps,
        loadJobDetails: sinon.stub().returns(
          Promise.resolve({
            meta: {
              jobId: commonProps.jobId,
            },
            error: true,
            payload: {
              status: 404,
            },
          })
        ),
        updateViewState: () => {
          // we expect updateViewState to get called
          done();
        },
      };

      const wrapper = shallow(<JobDetailsWrapper {...props} />);
      wrapper.instance().componentWillUnmount();
    });
  });
});
