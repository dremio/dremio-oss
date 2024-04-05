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

import { NEXT_ACTIONS } from "actions/explore/nextAction";
import { ExploreInfoHeader } from "./ExploreInfoHeader";

describe("ExploreInfoHeader", () => {
  let commonProps;
  let context;
  let wrapper;
  let instance;
  beforeEach(() => {
    commonProps = {
      pageType: "default",
      exploreViewState: Immutable.fromJS({}),
      dataset: Immutable.fromJS({
        datasetVersion: "11",
        tipVersion: "22",
        sql: "23",
        fullPath: ["newSpace", "newTable"],
        displayFullPath: ["displaySpace", "displayTable"],
        datasetType: "VIRTUAL_DATASET",
        apiLinks: {
          namespaceEntity: "/home/displaySpace/dataset/displayTable",
        },
      }),
      currentSql: "12",
      queryContext: Immutable.List(),
      routeParams: {
        tableId: "newTable",
        resourceId: "newSpace",
        resources: "space",
      },
      location: {
        pathname: "ds1",
        query: {
          tipVersion: "22",
          version: "22",
          mode: "edit",
        },
      },
      tableColumns: Immutable.fromJS([{ type: "INTEGER" }, { type: "TEXT" }]),
      activeScript: {},
      saveDataset: sinon.stub().returns(Promise.resolve("saveAsDataset")),
      saveAsDataset: sinon.stub().returns(Promise.resolve("saveAsDataset")),
      runTableTransform: sinon
        .stub()
        .returns(Promise.resolve("runTableTransform")),
      performNextAction: sinon
        .stub()
        .returns(Promise.resolve("performNextAction")),
      performTransformAndRun: sinon.spy(),
      startDownloadDataset: sinon.spy(),
      toggleRightTree: sinon.spy(),
      runDataset: sinon.spy(),
      runDatasetSql: sinon.spy(),
      previewDatasetSql: sinon.spy(),
      performTransform: sinon
        .stub()
        .returns(Promise.resolve("performTransform")),
      transformHistoryCheck: sinon.spy(),
      performLoadDataset: sinon
        .stub()
        .returns(Promise.resolve("performLoadDataset")),
      navigateToNextDataset: sinon.stub().returns("navigateToNextDataset"),
    };
    context = {
      router: { push: sinon.spy() },
      routeParams: {
        tableId: "newTable",
        resourceId: "newSpace",
        resources: "space",
      },
      location: {
        pathname: "pathname",
        query: {
          version: "123456",
          mode: "edit",
        },
      },
    };
    wrapper = shallow(<ExploreInfoHeader {...commonProps} />, { context });
    instance = wrapper.instance();
  });

  describe("rendering", () => {
    it("should render .explore-info-header", () => {
      expect(wrapper.hasClass("explore-info-header")).to.equal(true);
    });
  });

  describe("isEditedDataset", () => {
    it("should return false if dataset.datasetType is missing", () => {
      wrapper.setProps({
        dataset: commonProps.dataset.set("datasetType", undefined),
      });
      expect(instance.isEditedDataset()).to.be.false;
    });

    it("should return false if datasetType starts with PHYSICAL_DATASET", () => {
      wrapper.setProps({
        dataset: Immutable.fromJS({
          datasetType: "PHYSICAL_DATASET",
        }),
      });
      expect(instance.isEditedDataset()).to.be.false;
    });

    it('should always return false for "New Query"', () => {
      const dataset = commonProps.dataset.setIn(["displayFullPath", 0], "tmp");
      wrapper.setProps({ dataset });
      expect(instance.isEditedDataset()).to.be.false;
      // verify difference between tipVersion and initialDatasetVersion
      wrapper.setProps({
        currentSql: commonProps.dataset.get("sql"),
        initialDatasetVersion: "1",
      });
      expect(instance.isEditedDataset()).to.be.false;
    });

    it("should return true if dataset sql is different from currentSql", () => {
      wrapper.setProps({ currentSql: "different sql" });
      expect(instance.isEditedDataset()).to.be.true;
    });

    it("should return history.isEdited if none of the above are true", () => {
      wrapper.setProps({
        currentSql: commonProps.dataset.get("sql"),
        history: undefined,
      });
      expect(instance.isEditedDataset()).to.be.false;
      wrapper.setProps({ currentSql: null });
      expect(instance.isEditedDataset()).to.be.false;
      wrapper.setProps({ history: Immutable.Map({ isEdited: false }) });
      expect(instance.isEditedDataset()).to.be.false;
      wrapper.setProps({ history: Immutable.Map({ isEdited: true }) });
      expect(instance.isEditedDataset()).to.be.true;
    });
  });

  describe("#renderLeftPartOfHeader", () => {
    it("should render an empty div if !dataset.datasetType", () => {
      const node = shallow(
        instance.renderLeftPartOfHeader(
          commonProps.dataset.delete("datasetType"),
          false,
        ),
      );
      expect(node.find("div")).to.have.length(1);
    });

    it("should render dataset label when dataset has datasetType present", () => {
      const node = shallow(
        instance.renderLeftPartOfHeader(commonProps.dataset, false),
      );
      expect(node.find(".title-wrap").children()).to.have.length(1);
    });
  });

  describe("#renderDatasetLabel", () => {
    it("should render (edited) dataset label when isEditedDataset returns true", () => {
      sinon.stub(instance, "isEditedDataset").returns(true);
      const node = shallow(instance.renderDatasetLabel(commonProps.dataset));
      expect(node.find("FontIcon")).to.have.length(1);
      expect(node.find("EllipsedText").props().text).to.be.contains(
        'New Query{"0":{"id":"Dataset.Edited"}}',
      );
    });

    it("should not render (edited) dataset label when isEditedDataset returns false", () => {
      sinon.stub(instance, "isEditedDataset").returns(false);
      const node = shallow(instance.renderDatasetLabel(commonProps.dataset));
      expect(node.find("FontIcon")).to.have.length(1);
      expect(node.find("EllipsedText").props().text).to.be.contains(
        "New Query",
      );
      expect(node.find("EllipsedText").props().text).to.be.not.contains(
        "(edited)",
      );
    });
  });
});
