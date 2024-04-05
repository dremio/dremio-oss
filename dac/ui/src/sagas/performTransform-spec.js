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
import { call, put } from "redux-saga/effects";
import Immutable from "immutable";
import {
  runDataset,
  transformAndRunDataset,
} from "actions/explore/dataset/run";
import {
  newUntitledSql,
  newUntitledSqlAndRun,
} from "actions/explore/dataset/new";
import { runTableTransform } from "actions/explore/dataset/transform";
import { resetViewState } from "actions/resources";
import { transformHistoryCheck } from "sagas/transformHistoryCheck";

import { EXPLORE_TABLE_ID } from "reducers/explore/view";

import { transformThenNavigate } from "./transformWatcher";
import { loadDataset, cancelDataLoad } from "./performLoadDataset";

import {
  getTransformData,
  performTransform,
  handleRunDatasetSql,
  getFetchDatasetMetaAction,
  proceedWithDataLoad,
  performTransformSingle,
  getParsedSql,
} from "./performTransform";

describe("performTransform saga", () => {
  let dataset;
  let transformData;
  let gen;
  let next;
  const viewId = "VIEW_ID";
  const oldDatasetVersion = "oldId";

  beforeEach(() => {
    transformData = Immutable.fromJS({ href: "123" });
    dataset = Immutable.fromJS({
      displayFullPath: ["foo", "bar"],
      datasetVersion: oldDatasetVersion,
      sql: "select * from foo",
      context: ["fooContext"],
    });
  });

  describe("performTransformSingle", () => {
    const gotToCallback = () => {
      const apiAction = "an api action";
      next = gen.next(); // getFetchDatasetMetaAction call
      next = gen.next({ apiAction }); // cancelDataLoad call
      expect(next.value).to.be.eql(call(cancelDataLoad));
      next = gen.next(); // initializeExploreJobProgress
      next = gen.next(); // transformThenNavigate call
      expect(next.value).to.be.eql(
        call(transformThenNavigate, apiAction, undefined, undefined),
      );
      next = gen.next(); // focusSqlEditorSaga
    };

    it("should call callback with true if transform request succeeds", () => {
      const params = {
        dataset,
        currentSql: dataset.get("sql"),
        transformData,
        callback: sinon.spy(),
      };
      const query = { sqlStatement: dataset.get("sql"), cancelled: false };

      gen = performTransformSingle(params, query);

      gotToCallback();

      next = gen.next();
      expect(next.done).to.be.true;
    });

    it("should call callback with false if no transform necessary", () => {
      const params = {
        dataset,
        currentSql: dataset.get("sql"),
        callback: sinon.spy(),
      };
      const query = { sqlStatement: dataset.get("sql"), cancelled: false };

      gen = performTransformSingle(params, query);
      next = gen.next(); // getFetchDatasetMetaAction call
      next = gen.next({ apiAction: null }); // callback call

      expect(next.value).to.eql(call(params.callback, false, dataset));
      next = gen.next();
      expect(next.done).to.be.true;
    });
  });

  describe("handleRunDatasetSql", () => {
    let dataSet, exploreViewState, exploreState;
    beforeEach(() => {
      dataSet = Immutable.fromJS({ datasetVersion: 1, sql: "" });
      exploreViewState = Immutable.fromJS({ viewId: "a" });
      exploreState = { view: { currentSql: "", queryContext: [] } };
    });

    it("should do nothing if proceedWithDataLoad yields false", () => {
      gen = handleRunDatasetSql({ selectedSql: { sql: "", range: {} } });
      next = gen.next();
      next = gen.next(dataSet);
      next = gen.next(exploreViewState);
      next = gen.next(exploreState);

      expect(next.value).to.be.eql(
        call(
          proceedWithDataLoad,
          dataSet,
          exploreState.view.queryContext,
          exploreState.view.currentSql,
        ),
      );
      expect(gen.next(false).done).to.be.true;
    });

    it("should call perform transform with proper run parameters", () => {
      // needsTransform inside handleRunDatasetSql should return false with default items set in beforeEach
      const performTransformParam = {
        dataset: dataSet,
        currentSql: exploreState.view.currentSql,
        queryContext: exploreState.view.queryContext,
        viewId: exploreViewState.get("viewId"),
        isRun: true,
        runningSql: "",
        useOptimizedJobFlow: undefined,
        activeScriptId: undefined,
        selectedRange: undefined,
      };
      gen = handleRunDatasetSql({ selectedSql: { sql: "", range: {} } });
      next = gen.next(); // yield dataset
      next = gen.next(dataSet); // use dataset, yield exploreViewState
      next = gen.next(exploreViewState); // use exploreViewState, yield exploreState
      next = gen.next(exploreState); // use exploreState, yield call performTransform
      next = gen.next(true); // result for proceedWithDataLoad
      next = gen.next();
      expect(next.value).to.be.eql(
        call(performTransform, performTransformParam),
      );
    });

    it("should call perform transform with proper preview parameters", () => {
      // needsTransform inside handleRunDatasetSql should return false with default items set in beforeEach
      const performTransformParam = {
        dataset: dataSet,
        currentSql: exploreState.view.currentSql,
        queryContext: exploreState.view.queryContext,
        viewId: exploreViewState.get("viewId"),
        forceDataLoad: true,
        runningSql: "",
        useOptimizedJobFlow: undefined,
        activeScriptId: undefined,
        selectedRange: undefined,
      };
      gen = handleRunDatasetSql({
        isPreview: true,
        selectedSql: { sql: "", range: {} },
      });
      next = gen.next(); // yield dataset
      next = gen.next(dataSet); // use dataset, yield exploreViewState
      next = gen.next(exploreViewState); // use exploreViewState, yield exploreState
      next = gen.next(exploreState); // use exploreState, yield call performTransform
      next = gen.next(true); // result for proceedWithDataLoad
      next = gen.next();
      expect(next.value).to.be.eql(
        call(performTransform, performTransformParam),
      );
    });
  });

  describe("getFetchDatasetMetaAction", () => {
    const currentSql = "select * from foo";
    const queryContext = Immutable.fromJS(["@dremio"]);

    it("should get sql from dataset if currentSql arg is falsy", () => {
      const payload = {
        dataset,
        currentSql: undefined,
      };

      gen = getFetchDatasetMetaAction(payload);
      next = gen.next();
      next = gen.next();
      expect(next.value).to.eql(
        call(
          getTransformData,
          payload.dataset,
          "select * from foo",
          undefined,
          undefined,
          undefined,
        ),
      );
    });

    const goToTransformData = (transformDataResult) => {
      next = gen.next(); // skip getTransformData
      next = gen.next(); // skip nessieReferences
      next = gen.next(transformDataResult);
    };

    describe("Run dataset case", () => {
      it("should do newUntitledSqlAndRun if no dataset version", () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset: dataset.remove("datasetVersion"),
          currentSql,
          queryContext,
          viewId,
        });
        goToTransformData();
        const newVersion = next.value.CALL.args[5];
        expect(next.value).to.be.eql(
          call(
            newUntitledSqlAndRun,
            currentSql,
            queryContext,
            viewId,
            undefined,
            "",
            newVersion,
            false,
          ),
        );

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { changePathname: true }, //changePathname to navigate to newUntitled,
          newVersion,
        });
      });

      it("should do transformAndRun if transformData", () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset,
          currentSql,
          queryContext,
          viewId,
        });
        goToTransformData(transformData);
        expect(next.value).to.be.eql(put(resetViewState(EXPLORE_TABLE_ID)));
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql(
          call(transformAndRunDataset, dataset, transformData, viewId, ""),
        );

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: undefined,
          newVersion: undefined,
        });
      });

      it("should do runDataset if neither of the above are true", () => {
        gen = getFetchDatasetMetaAction({
          isRun: true,
          dataset,
          currentSql,
          queryContext,
          viewId,
        });
        goToTransformData();
        expect(next.value).to.be.eql(call(runDataset, dataset, viewId, ""));

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { replaceNav: true, preserveTip: true },
          newVersion: undefined,
        });
      });
    });

    describe("Preview dataset case", () => {
      it("should call newUntitledSql if dataset has not been created", () => {
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset: dataset.remove("datasetVersion"),
          currentSql,
          queryContext,
          viewId,
        });
        goToTransformData();
        const newVersion = next.value.CALL.args[5];
        expect(next.value).to.be.eql(
          call(
            newUntitledSql,
            currentSql,
            queryContext.toJS(),
            viewId,
            undefined,
            "",
            newVersion,
            false,
          ),
        );

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { changePathname: true }, //changePathname to navigate to newUntitled,
          newVersion,
        });
      });

      it("should call runTableTransform if transform data is provided", () => {
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset,
          currentSql,
          queryContext,
          viewId,
        });
        goToTransformData(transformData);
        expect(next.value).to.be.eql(
          call(
            runTableTransform,
            dataset,
            transformData,
            viewId,
            undefined,
            "",
          ),
        );

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: undefined,
          newVersion: undefined,
        });
      });

      it("should call loadDataset if it is existent dataset without transformation is passed as argument along with forceDataLoad = true", () => {
        const forceDataLoad = true;
        gen = getFetchDatasetMetaAction({
          isRun: false,
          dataset,
          currentSql,
          queryContext,
          viewId,
          forceDataLoad,
        });
        goToTransformData();
        expect(next.value).to.be.eql(
          call(loadDataset, dataset, viewId, forceDataLoad, "", true),
        );

        const mockApiAction = "mock api call";
        next = gen.next(mockApiAction);
        expect(next.value).to.be.eql({
          apiAction: mockApiAction,
          navigateOptions: { replaceNav: true, preserveTip: true },
          newVersion: undefined,
        });
      });
    });
  });

  describe("getTransformData", () => {
    const sql = "select * from foo";
    const context = Immutable.List(["@dremio"]);
    it("should return supplied transformData if present", () => {
      expect(getTransformData(dataset, sql, context, transformData)).to.equal(
        transformData,
      );
    });

    it("should return undefined if dataset isNewQuery and no transformData supplied", () => {
      expect(getTransformData(dataset.set("isNewQuery", true), sql, context)).to
        .be.undefined;
    });

    it("should return updateSQL only if dataset is not new, no transformData supplied, and sql has changed", () => {
      expect(getTransformData(dataset, sql, context)).to.be.eql({
        type: "updateSQL",
        sql,
        sqlContextList: context.toJS(),
        referencesList: [],
      });

      expect(getTransformData(dataset.set("isNewQuery", true), sql, context)).to
        .be.undefined;
      expect(getTransformData(dataset, sql, context, transformData)).to.equal(
        transformData,
      );
      expect(getTransformData(dataset, null, dataset.get("context"))).to.be
        .undefined;
      expect(
        getTransformData(dataset, dataset.get("sql"), dataset.get("context")),
      ).to.be.undefined;
    });

    it("should not fail if dataset is missing context", () => {
      expect(
        getTransformData(dataset.remove("context"), sql, context),
      ).to.be.eql({
        type: "updateSQL",
        sql,
        sqlContextList: context.toJS(),
        referencesList: [],
      });
    });
  });

  describe("proceedWithDataLoad", () => {
    it("returns true if sql and context is not changed", () => {
      gen = proceedWithDataLoad(
        dataset,
        dataset.get("context"),
        dataset.get("sql"),
      );
      expect(gen.next()).to.be.eql({
        done: true,
        value: true,
      });
    });
    const testTransformHistoryCheck = (resultValue) =>
      it(`return ${resultValue}, if transformHistoryCheck returns ${resultValue}`, () => {
        gen = proceedWithDataLoad(
          dataset,
          dataset.get("context"),
          "select * from other_query",
        );
        expect(gen.next().value).to.be.eql(
          call(transformHistoryCheck, dataset),
        );
        expect(gen.next(resultValue)).to.be.eql({
          done: true,
          value: resultValue,
        });
      });
    [true, false].map(testTransformHistoryCheck);
  });

  describe("getParsedSql", () => {
    it("returns datasetSql if currentSql and runningSql are undefined", () => {
      const params = {
        dataset,
        currentSql: undefined,
        runningSql: undefined,
      };

      const [sql] = getParsedSql(params);
      expect(sql).to.eql(["select * from foo"]);
    });

    it("returns currentSql if provided and runningSql is undefined", () => {
      const params = {
        dataset,
        currentSql: "select 1; select 2",
        runningSql: undefined,
      };

      const [sql] = getParsedSql(params);
      expect(sql).to.eql(["select 1", " select 2"]);
    });

    it("returns runningSql if provided", () => {
      const params = {
        dataset,
        currentSql: "select 1; select 2",
        runningSql: "select 1",
      };

      const [sql] = getParsedSql(params);
      expect(sql).to.eql(["select 1"]);
    });
  });
});
