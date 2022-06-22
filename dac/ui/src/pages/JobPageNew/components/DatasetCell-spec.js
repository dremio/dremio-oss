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
import { mount } from "enzyme";
import DatasetCell from "./DatasetCell";

describe("DatasetCell", () => {
  let commonProps;
  const context = { loggedInUser: {} };
  beforeEach(() => {
    commonProps = {
      job: Immutable.fromJS({
        queriedDatasets: [
          {
            datasetName: "Samples.samples.dremio.com.SF weather 2018-2019.csv",
            datasetPath: "Samples.samples.dremio.com.SF weather 2018-2019.csv",
            datasetType: "PHYSICAL_DATASET_SOURCE_FILE",
          },
        ],
      }),
    };
  });

  it("should render with props without exploding", () => {
    const wrapper = mount(<DatasetCell {...commonProps} />, { context });
    expect(wrapper).to.have.length(1);
  });
});
