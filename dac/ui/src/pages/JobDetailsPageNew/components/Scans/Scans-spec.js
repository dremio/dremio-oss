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
import { mount, shallow } from "enzyme";
import Sinon from "sinon";
import Immutable from "immutable";
import Scans from "./Scans";

describe("Scans", () => {
  const minimalProps = {
    scansForFilter: [],
    scans: new Immutable.List(),
  };

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<Scans {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it("renders child correctly", () => {
    const scansForFilter = [
      { label: "Scans.SourceType", content: "Managed Reflection(Parque)" },
      { label: "Scans.ScanThread", content: "115" },
      { label: "Scans.ScanTime", content: "00:00:01.50" },
      { label: "Scans.IoWaitTime", content: "00:00:00.75" },
      { label: "Scans.RowScanned", content: "143K" },
      { label: "Scans.FilesScanned", content: "796" },
      { label: "Scans.SplitsScanned", content: "3753" },
      { label: "Scans.PruningRatio", content: "65%" },
      { label: "Scans.RuntimeFiltering", content: "No" },
      { label: "Scans.LocalityPercentage", content: "N/A" },
      { label: "Scans.FilterPushdown", content: "Managed Reflection(Parque)" },
    ];
    const scans = Immutable.fromJS([
      {
        sourceType: "Managed Reflection(Parque)",
        nrScanThreads: "115",
        totalScanDurationMs: "00:00:01.50",
        ioWaitDurationMs: "00:00:00.75",
        nrScannedRows: "143K",
        filterScanned: "796",
        splitsScanned: "3753",
        pruningRatio: "65%",
        runtimeFilterApplied: "No",
        loyaltyPercentage: "N/A",
        pushdownFilter: "Managed Reflection(Parque)",
      },
    ]);
    const intl = { formatMessage: Sinon.spy() };
    const wrapper = mount(
      <Scans scansForFilter={scansForFilter} scans={scans} intl={intl} />
    );
    const account = wrapper
      .find('[data-qa="dropdown-customer"]')
      .simulate("click");
    expect(account).to.have.length(1);
  });
});
