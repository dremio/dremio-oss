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
import { fireEvent } from "@testing-library/react";
import Immutable from "immutable";
import { TestSqlScripts as SQLScripts } from "./SQLScripts";

const testScripts = [
  {
    name: "NewScript1",
    createdAt: 1643332135184,
    createdBy: "d13106f2-bd04-4bfe-aba7-e32d8e2f629a",
    description: "",
    modifiedAt: 1643335392379,
    modifiedBy: "d13106f2-bd04-4bfe-aba7-e32d8e2f629a",
    context: ["@dremio"],
    content: "SELECT * FROM NotaKoreanName",
    id: "b8233efa-0df2-4a53-bc79-d4ee8345d151",
  },
  {
    name: "NewQuery2",
    createdAt: 1643331853618,
    createdBy: "d13106f2-bd04-4bfe-aba7-e32d8e2f629a",
    description: "",
    modifiedAt: 1643332110158,
    modifiedBy: "d13106f2-bd04-4bfe-aba7-e32d8e2f629a",
    context: [
      "@dremio",
      "TestingsPath",
      "Hello",
      "OceanBottom",
      "OceanVolcanoes",
    ],
    content: "SELECT * FROM EarthCore limit 4",
    id: "3b81385d-d044-4247-acca-83855207148a",
  },
];

describe("SQLScripts", () => {
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      scripts: testScripts,
      mineScripts: testScripts,
      user: Immutable.Map(),
      activeScript: {},
      currentSql: null,
      fetchSQLScripts: sinon.spy(),
      updateSQLScript: sinon.spy(),
      deleteScript: sinon.spy(),
      setActiveScript: sinon.spy(),
      showConfirmationDialog: sinon.spy(),
      router: { push: sinon.spy() },
      resetQueryState: sinon.spy(),
    };
  });

  it.skip("should render with default/empty configs", () => {
    const { container, getByTestId, queryByText } = render(
      <SQLScripts {...minimalProps} scripts={[]} />
    );
    expect(getByTestId("sqlScripts")).to.exist;
    expect(queryByText("Date")).to.exist;
    expect(container.querySelector(".--sortArrow.--desc")).to.exist;
    expect(container.querySelector(".sqlScripts__empty")).to.exist;
  });

  it.skip("should render the scripts, none selected", () => {
    const { container, getByTestId } = render(<SQLScripts {...minimalProps} />);
    expect(getByTestId("sqlScripts")).to.exist;
    expect(container.querySelector(".MuiList-root")).to.exist;
    expect(
      container.querySelectorAll(".sqlScripts__menu-item").length
    ).to.equal(2);
    expect(container.querySelector(".sqlScripts__menu-item.--selected")).to.not
      .exist;
  });

  // keyDown fireEvent isn't working
  it.skip("should render the filter the scripts by search key", async () => {
    const { container, getByTestId, getByText, queryByText } = render(
      <SQLScripts {...minimalProps} />
    );
    expect(getByTestId("sqlScripts")).to.exist;
    expect(container.querySelector(".MuiList-root")).to.exist;
    expect(
      container.querySelectorAll(".sqlScripts__menu-item").length
    ).to.equal(2);
    expect(getByText("NewScript1")).to.exist;
    expect(getByText("NewQuery2")).to.exist;

    fireEvent.keyDown(getByTestId("sqlScripts__search"), {
      key: "s",
      code: "KeyS",
    });
    expect(
      container.querySelectorAll(".sqlScripts__menu-item").length
    ).to.equal(1);
    expect(getByText("NewScript1")).to.exist;
    expect(queryByText("NewQuery2")).to.not.exist;
  });

  // could only get setActiveScript to be recognized by excluding connect from SQLScripts
  it.skip("should select a script", () => {
    const { container } = render(<SQLScripts {...minimalProps} />);
    expect(container.querySelector(".MuiList-root")).to.exist;
    expect(
      container.querySelectorAll(".sqlScripts__menu-item").length
    ).to.equal(2);
    fireEvent.click(container.querySelectorAll(".sqlScripts__menu-item")[0]);
    expect(minimalProps.setActiveScript).to.be.calledWith({
      script: {
        ...testScripts[0],
        colors: { backgroundColor: "#D02362", color: "#FFF" },
        initials: "",
      },
    });
  });
});
