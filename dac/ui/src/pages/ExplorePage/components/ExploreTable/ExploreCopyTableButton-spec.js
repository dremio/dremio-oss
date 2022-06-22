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

import { ExploreCopyTableButton } from "./ExploreCopyTableButton";

describe("ExploreCopyTableButton", () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {
      addNotification: () => {},
      location: {},
      tableData: Immutable.fromJS({
        columns: [{ name: "col1" }, { name: "col2" }],
        rows: [
          { row: [{ v: "val1-1" }, { v: "val1-2" }] },
          { row: [{ v: "val2-1" }, { v: "val2-2" }] },
        ],
      }),
    };
    commonProps = {
      ...minimalProps,
    };
  });

  it("should render with minimal props without exploding", () => {
    const wrapper = shallow(<ExploreCopyTableButton {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  it("should render with common props without exploding", () => {
    const wrapper = shallow(<ExploreCopyTableButton {...commonProps} />);
    expect(wrapper).to.have.length(1);
  });

  it("should make copy text from tableData", () => {
    const result = ExploreCopyTableButton.makeCopyTextFromTableData(
      commonProps.tableData.toJS()
    );
    const colRow = "col1\tcol2";
    expect(result.startsWith(colRow)).to.equal(true);
    expect(result.endsWith("val2-1\tval2-2")).to.equal(true);
    expect(result.substring(colRow.length, colRow.length + 2)).to.equal("\r\n");
  });

  it("should escape double quoutes", () => {
    const tableDataObj = commonProps.tableData.toJS();
    tableDataObj.rows[1].row[1].v = 'val\t"2-2"';
    const result =
      ExploreCopyTableButton.makeCopyTextFromTableData(tableDataObj);
    expect(result.endsWith('val2-1\t"val\t""2-2"""')).to.equal(true);
  });
});
