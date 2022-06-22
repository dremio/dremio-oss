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
import Summary from "./Summary";

describe("Summary", () => {
  const minimalProps = {
    jobSummary: [],
  };
  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<Summary {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });
  it("renders child correctly", () => {
    const jobSummary = [
      { label: "Job.QueryType", content: "UI(run)" },
      { label: "Job.StartTime", content: "14/04/2021 5:30" },
      { label: "Job.Duration", content: "14.0s" },
      { label: "Common.User", content: "satheesh@dremio" },
      { label: "Common.Queue", content: "High Cost User Queries" },
      { label: "Job.Summary.Input", content: "1.2GB/1.5m Records" },
      { label: "Job.Summary.Output", content: "1.7MB/13k Records" },
      { label: "Job.Summary.WaitOnClient", content: "<1s" },
    ];
    const wrapper = shallow(<Summary jobSummary={jobSummary} />);
    expect(wrapper.find(".summary__content").length).equal(jobSummary.length);
  });
});
