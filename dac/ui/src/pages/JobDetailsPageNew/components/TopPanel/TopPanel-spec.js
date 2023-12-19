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
import { render } from "rtlUtils";

import { TopPanel } from "./TopPanel";

describe("TopPanel", () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      changePages: sinon.spy(),
      jobId: "456",
      jobStatus: "COMPLETED",
      router: {},
      location: {},
      jobDetails: Immutable.fromJS({
        attemptDetails: [
          {
            profileUrl: "",
          },
        ],
        queriedDatasets: [{ datasetPathsList: [], datasetType: "" }],
      }),
      renderTabs: () => null,
    };
  });

  it("should render with minimal props without exploding", () => {
    const { container } = render(<TopPanel {...minimalProps} />);
    expect(container.getElementsByClassName("topPanel")).to.have.length(1);
  });

  it("should render the jobid", () => {
    const props = {
      ...minimalProps,
      jobId: "293",
    };
    const { getByTestId } = render(<TopPanel {...props} />);

    const element = getByTestId("top-panel-jobId");
    expect(element.innerHTML.includes("293")).to.equal(true);
  });

  it("should render the correct Open Results link", () => {
    const path = ["@dremio", "SF_Incidents_16"];
    const encodedPath = path.map((item) => encodeURIComponent(item));
    const props = {
      ...minimalProps,
      jobId: "293",
      jobDetails: Immutable.fromJS({
        attemptDetails: [
          {
            profileUrl: "",
          },
        ],
        datasetPaths: path,
        resultsAvailable: true,
        queryType: "UI_RUN",
        queriedDatasets: [{ datasetType: "" }],
      }),
    };

    const wrapper = shallow(<TopPanel {...props} />);
    const element = wrapper.find('[data-qa="open-results-link"]');
    expect(element.props().to.pathname).to.equal(
      `/space/${encodedPath[0]}/${encodedPath[1]}`
    );
  });

  it("should render the correct Open Results link, with nested path", () => {
    const path = ["@dremio", "folder1", "folder2@", "SF_Incidents_16"];
    const encodedPath = path.map((item) => encodeURIComponent(item));
    const props = {
      ...minimalProps,
      jobId: "293",
      jobDetails: Immutable.fromJS({
        attemptDetails: [
          {
            profileUrl: "",
          },
        ],
        datasetPaths: path,
        resultsAvailable: true,
        queryType: "UI_RUN",
        queriedDatasets: [{ datasetType: "" }],
      }),
    };

    const wrapper = shallow(<TopPanel {...props} />);
    const element = wrapper.find('[data-qa="open-results-link"]');
    expect(element.props().to.pathname).to.equal(
      `/space/${encodedPath[0]}/${encodedPath[1]}.${encodedPath[2]}.${encodedPath[3]}`
    );
  });

  it("should render the cancel button if page is running", () => {
    const props = {
      ...minimalProps,
      jobId: "293",
      jobStatus: "RUNNING",
    };
    const { getByText } = render(<TopPanel {...props} />);

    expect(getByText('{"0":{"id":"Common.Cancel"}}')).to.exist;
  });

  it("should not render the Open Results button", () => {
    const props = {
      ...minimalProps,
      jobId: "293",
      jobDetails: Immutable.fromJS({
        resultsAvailable: false,
      }),
    };

    const { container } = render(<TopPanel {...props} />);
    const element = container.getElementsByClassName(
      "topPanel__openResults"
    )[0];
    expect(element.children.length).to.equal(0);
  });
});
