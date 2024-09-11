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
// import { expect } from "chai";
import Immutable from "immutable";
// @ts-ignore
import { render } from "rtlUtils";
// @ts-ignore
import sinon from "sinon";
import { expect } from "chai";
import { fireEvent } from "@testing-library/react";

import {
  ResourceTreeContainer,
  ResourceTreeContainerProps,
} from "./ResourceTreeContainer";

describe("ResourceTreeContainer", () => {
  let minimalProps: ResourceTreeContainerProps;
  let propsForChildren: {
    intl: {
      formatMessage: () => void;
    };
  };
  beforeEach(() => {
    minimalProps = {
      starredItems: [],
      starredResourceTree: Immutable.fromJS([]),
      resourceTree: Immutable.fromJS([
        {
          fullPath: ["bing bang boom"],
          id: "78d45206-0429-4726-99a3-7bf713ee8234",
          name: "bing bang boom",
          type: "SPACE",
          url: "/resourcetree/%22bing%20bang%20boom%22",
        },
      ]),
      nessie: {},
      dragType: "explorePage",
      sidebarCollapsed: false,
      isCollapsable: true,
      browser: true,
      isExpandable: true,
      shouldShowOverlay: true,
      shouldAllowAdd: true,
      isSqlEditorTab: true,
      style: { maxHeight: "initial", minHeight: "initial" },
      insertFullPathAtCursor: () => {},
      handleSidebarCollapse: () => {},
      dispatchLoadResourceTree: sinon.stub().returns({
        then: (arg: any) => "LoadResourceTree",
      }),
      dispatchFetchScripts: sinon.stub().returns({
        then: (arg: any) => "FetchScripts",
      }),
      dispatchSetActiveScript: sinon.stub().returns({
        then: (arg: any) => "SetActiveScript",
      }),
      dispatchLoadSummaryDataset: sinon.stub().returns({
        then: (arg: any) => "LoadSummaryDataset",
      }),
      dispatchLoadStarredResources: sinon.stub().returns({
        then: (arg: any) => "LoadStarredResources",
      }),
      dispatchStarItem: sinon.stub().returns({
        then: (arg: any) => "StarItem",
      }),
      dispatchUnstarItem: sinon.stub().returns({
        then: (arg: any) => "UnstarItem",
      }),
    } as ResourceTreeContainerProps;
    propsForChildren = {
      intl: {
        formatMessage: () => {},
      },
    };
  });
  it("changes the tab when invoked", () => {
    // ResourceTreeContainer
    const { getByTestId } = render(<ResourceTreeContainer {...minimalProps} />);
    expect(getByTestId("resourceTreeContainer")).to.exist;
  });
  describe("handleStarredTabChange", () => {
    it("changes the tab when invoked", () => {
      const props = { ...minimalProps, ...propsForChildren };
      const { getByTestId } = render(<ResourceTreeContainer {...props} />);
      expect(getByTestId('{"0":{"id":"Resource.Tree.All"}}subHeaderTab')).to
        .exist;
      expect(getByTestId('{"0":{"id":"Resource.Tree.Starred"}}subHeaderTab')).to
        .exist;
      fireEvent.click(
        getByTestId('{"0":{"id":"Resource.Tree.Starred"}}subHeaderTab'),
      );
      expect(
        getByTestId(
          '{"0":{"id":"Resource.Tree.Starred"}}subHeaderTab--selected',
        ),
      ).to.exist;
      expect(getByTestId('{"0":{"id":"Resource.Tree.All"}}subHeaderTab')).to
        .exist;
      fireEvent.click(
        getByTestId('{"0":{"id":"Resource.Tree.All"}}subHeaderTab'),
      );
      expect(
        getByTestId('{"0":{"id":"Resource.Tree.All"}}subHeaderTab--selected'),
      ).to.exist;
      expect(getByTestId('{"0":{"id":"Resource.Tree.Starred"}}subHeaderTab')).to
        .exist;
    });
  });
});
