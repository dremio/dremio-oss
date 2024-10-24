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
import { render } from "rtlUtils";
import Immutable from "immutable";
import { TestScrollableTable as ScrollableTable } from "./ScrollableTable";

const testColumns = [
  {
    key: "jobStatus",
    id: "jobStatus",
    label: "",
    disableSort: true,
    isSelected: true,
    width: 40,
    minWidth: 40,
    headerStyle: { paddingLeft: "12px" },
    style: { paddingLeft: "12px" },
    isFixedWidth: true,
    disabled: true,
  },
  {
    key: "job",
    id: "job",
    label: "Job ID",
    disableSort: false,
    isSelected: true,
    width: 200,
    minWidth: 120,
    headerStyle: { paddingLeft: "12px" },
    style: { paddingLeft: "12px" },
    isFixedWidth: true,
    disabled: true,
  },
];

const jobStatusFunction = () => {
  return <div>Running</div>;
};

const jobFunction = () => {
  return <div>1dc4bf86-b201-c688-bac5-63d5c388be00</div>;
};

const testTableData = [
  {
    data: {
      jobStatus: {
        node: () => jobStatusFunction(),
      },
      job: {
        node: () => jobFunction(),
      },
    },
  },
];

describe("ScrollableTable", () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      columns: testColumns,
      fixedColumnCount: 1,
      tableData: Immutable.List(testTableData),
    };
  });

  it("Basic table render with default/empty data but with columns", () => {
    const { container, getByTestId, queryByText } = render(
      <ScrollableTable {...minimalProps} />,
    );
    // table exists
    expect(getByTestId("scrollable-table-test")).to.exist;
    // column exists
    expect(queryByText("Job ID")).to.exist;
    // fixed column exists
    expect(
      container.querySelector(".ReactVirtualized__Grid__innerScrollContainer"),
    ).to.exist;
  });
  it("Resizing columns to not work if isFixedWidth is true", () => {
    const { container } = render(<ScrollableTable {...minimalProps} />);
    expect(container.querySelector("resizable-column-test")).to.not.exist;
  });
  it("Cells are rendered", () => {
    const { container } = render(<ScrollableTable {...minimalProps} />);
    expect(container.querySelector("table-cell-test")).to.not.exist;
  });
});
