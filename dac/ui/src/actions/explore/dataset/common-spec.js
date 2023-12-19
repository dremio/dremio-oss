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
import { replace } from "react-router-redux";
import Immutable from "immutable";

import * as Actions from "./common";

const datasetVersion = "123";
const pathname = "/home/dataset/foo";
const nextDatasetLink = "/home/dataset2/aNewDataset"; //should differs from pathname

const responsePayload = Immutable.fromJS({
  entities: {
    fullDataset: {
      [datasetVersion]: {},
    },
    datasetUI: {
      [datasetVersion]: {
        datasetVersion,
        links: {
          self: `${nextDatasetLink}?version=${encodeURIComponent(
            datasetVersion
          )}`,
        },
        apiLinks: {
          namespaceEntity: "/home/dataset2/file/aNewDataset",
        },
      },
    },
  },
  result: datasetVersion,
});

const emptyEntityState = {
  resources: {
    entities: new Immutable.Map({
      history: new Immutable.Map({}),
      historyItem: new Immutable.Map({}),
      datasetUI: new Immutable.Map({}),
    }),
  },
};

describe("common", () => {
  describe("navigateToNextDataset", () => {
    let getStore;
    let location;

    const testActionWithPathnameToSubpage = (pageType) => {
      const pathWithPageType = `${pathname}/${pageType}`;
      const getStateLocal = () => ({
        routing: {
          locationBeforeTransitions: {
            state: {},
            pathname: pathWithPageType,
          },
        },
        ...emptyEntityState,
      });
      const result = Actions.navigateToNextDataset({
        payload: responsePayload,
      })((obj) => obj, getStateLocal);
      expect(result).to.eql(
        replace({
          pathname: pathWithPageType,
          query: {
            version: datasetVersion,
            tipVersion: "123",
          },
          state: {},
        })
      );
    };

    beforeEach(() => {
      location = {
        query: { tipVersion: "tip123" },
        state: {},
        pathname, // should not be changed, unless changePathname or 'isSaveAs' is provided
      };
      getStore = () => ({
        routing: { locationBeforeTransitions: location },
        ...emptyEntityState,
      });
    });

    it("should return replace action", () => {
      const result = Actions.navigateToNextDataset({
        payload: responsePayload,
      })((obj) => obj, getStore);
      expect(result).to.eql(
        replace({
          pathname,
          query: {
            version: datasetVersion,
            tipVersion: "123",
          },
          state: {},
        })
      );
    });

    it("should return replace action with pathname to graph", () => {
      testActionWithPathnameToSubpage("graph");
    });

    it("should return replace action with pathname to wiki", () => {
      testActionWithPathnameToSubpage("wiki");
    });

    it("should return replace action with pathname to reflections", () => {
      testActionWithPathnameToSubpage("reflections");
    });

    it("should set jobId from getNextJobId", () => {
      const jobId = "someJobId";
      sinon.stub(Actions, "_getNextJobId").returns(jobId);
      const payload = responsePayload.setIn(
        ["entities", "fullDataset", datasetVersion, "jobId"],
        Immutable.Map({ id: jobId })
      );
      const result = Actions.navigateToNextDataset({ payload })(
        (obj) => obj,
        getStore
      );
      expect(result).to.eql(
        replace({
          pathname,
          query: {
            jobId,
            version: datasetVersion,
            tipVersion: "123",
          },
          state: {},
        })
      );
      Actions._getNextJobId.restore();
    });

    it("should fail when no dataset in response", () => {
      expect(() =>
        Actions.navigateToNextDataset({ payload: undefined })(
          (obj) => obj,
          getStore
        )
      ).to.throw(Error);

      expect(() =>
        Actions.navigateToNextDataset(
          { payload: responsePayload.set("result", undefined) },
          false
        )((obj) => obj, getStore)
      ).to.throw(Error);
    });

    describe("_getNextJobId", () => {
      it("should only include jobId if fullDataset is a run result (not approximate)", () => {
        const fullDataset = Immutable.fromJS({
          jobId: { id: "someJobId" },
        });
        expect(Actions._getNextJobId(fullDataset.set("approximate", true))).to
          .be.undefined;
        expect(
          Actions._getNextJobId(fullDataset.set("approximate", false))
        ).to.equal(fullDataset.getIn(["jobId", "id"]));
      });
    });

    describe("isSaveAs param", () => {
      it("should change query.mode to edit", () => {
        const result = Actions.navigateToNextDataset(
          {
            payload: responsePayload,
          },
          { isSaveAs: true }
        )((obj) => obj, getStore);
        expect(result).to.eql(
          replace({
            pathname: nextDatasetLink, // for 'save as' we should take a path from response
            query: {
              version: datasetVersion,
              mode: "edit",
              tipVersion: "123",
            },
            state: {
              afterDatasetSave: true,
            },
          })
        );
      });
    });

    it("should take pathname from a response if changePathname = true", () => {
      const result = Actions.navigateToNextDataset(
        {
          payload: responsePayload,
        },
        { changePathname: true }
      )((obj) => obj, getStore);
      expect(result).to.eql(
        replace({
          pathname: nextDatasetLink, // should take a path from response
          query: {
            version: datasetVersion,
            tipVersion: "123",
          },
          state: {},
        })
      );
    });

    describe("preserveTip param", () => {
      it("should preserve tipVersion from location", () => {
        location.query = { tipVersion: "abcde" };
        const result = Actions.navigateToNextDataset(
          {
            payload: responsePayload,
          },
          { preserveTip: true }
        )((obj) => obj, getStore);
        expect(result).to.eql(
          replace({
            pathname,
            query: {
              version: datasetVersion,
              tipVersion: "abcde",
            },
            state: {},
          })
        );
      });
    });
  });
});
