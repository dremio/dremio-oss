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
import Immutable from "immutable";

import { RUN_TABLE_TRANSFORM_START } from "actions/explore/dataset/common";
import {
  LOAD_NEXT_ROWS_SUCCESS,
  UPDATE_EXPLORE_JOB_PROGRESS,
  INIT_EXPLORE_JOB_PROGRESS,
  SET_EXPLORE_JOBID_IN_PROGRESS,
  UPDATE_EXPLORE_JOB_RECORDS,
} from "#oss/actions/explore/dataset/data";
import { UPDATE_COLUMN_FILTER } from "#oss/actions/explore/view";

import table from "./table";

describe("table", () => {
  const initialState = Immutable.fromJS({
    table: {},
  });

  it("returns unaltered state by default", () => {
    const result = table(initialState, { type: "bla" });
    expect(result).to.equal(initialState);
  });

  describe("RUN_TABLE_TRANSFORM_START", () => {
    const payload = Immutable.fromJS({
      entities: {
        dataset: {
          bla: {
            version: "123",
          },
        },
      },
    });

    it("should set table entity with contents of meta.nextTable", () => {
      const result = table(initialState, {
        type: RUN_TABLE_TRANSFORM_START,
        payload,
        meta: { nextTable: Immutable.fromJS({ table: 1, version: "123" }) },
      });
      expect(result.getIn(["tableData", "123"])).to.eql(
        Immutable.fromJS({ table: 1, version: "123" }),
      );
    });

    it("should not set table when !meta.nextTable ", () => {
      const result = table(initialState, {
        type: RUN_TABLE_TRANSFORM_START,
        payload,
        meta: { nextTable: null },
      });
      expect(result.getIn(["tableData", "123"])).to.be.undefined;
    });
  });

  describe("LOAD_NEXT_ROWS_SUCCESS", () => {
    const initialRows = Immutable.fromJS([
      {
        row: [
          { v: "String" },
          { v: "Numb" },
          { v: "Name" },
          { v: "customer_id" },
        ],
      },
      { row: [{ v: "initial" }, { v: 10 }, { v: "dremio" }, { v: 1001 }] },
      { row: [{ v: "initial1" }, { v: 11 }, { v: "dremio2" }, { v: 1002 }] },
      { row: [{ v: "initial2" }, { v: 12 }, { v: "dremio3" }, { v: 1003 }] },
    ]);
    const initRows = initialState.setIn(
      ["tableData", 123, "rows"],
      initialRows,
    );
    const payload = {
      rows: [{ row: [{ v: "bla" }, { v: "bla" }, { v: "bla" }, { v: "bla" }] }],
      columns: [{ type: "string", index: 0 }],
    };

    it("should merge table only with one row if we get offset == 0", () => {
      const result = table(initRows, {
        type: LOAD_NEXT_ROWS_SUCCESS,
        payload,
        meta: { datasetVersion: 123, offset: 0 },
      });
      expect(result.getIn(["tableData", 123, "rows"])).to.equal(
        Immutable.fromJS(payload.rows),
      );
    });

    it("should append new rows", () => {
      const result = table(initRows, {
        type: LOAD_NEXT_ROWS_SUCCESS,
        payload,
        meta: { datasetVersion: 123, offset: 4 },
      });
      expect(result.getIn(["tableData", 123, "rows"])).to.equal(
        initialRows.concat(Immutable.fromJS(payload.rows)),
      );
    });

    it("should replace last two rows to one new row", () => {
      const result = table(initRows, {
        type: LOAD_NEXT_ROWS_SUCCESS,
        payload,
        meta: { datasetVersion: 123, offset: 2 },
      });
      expect(result.getIn(["tableData", 123, "rows"])).to.equal(
        Immutable.fromJS([
          {
            row: [
              { v: "String" },
              { v: "Numb" },
              { v: "Name" },
              { v: "customer_id" },
            ],
          },
          { row: [{ v: "initial" }, { v: 10 }, { v: "dremio" }, { v: 1001 }] },
          ...payload.rows,
        ]),
      );
    });

    it("should set rows if no intial rows", () => {
      const result = table(initialState, {
        type: LOAD_NEXT_ROWS_SUCCESS,
        payload,
        meta: { datasetVersion: 123 },
      });
      expect(
        result
          .getIn(["tableData", 123, "rows"])
          .equals(Immutable.fromJS(payload.rows)),
      ).to.be.true;
    });

    it("should set columns", () => {
      const result = table(initialState, {
        type: LOAD_NEXT_ROWS_SUCCESS,
        payload,
        meta: { datasetVersion: 123 },
      });
      expect(
        result
          .getIn(["tableData", 123, "columns"])
          .equals(Immutable.fromJS(payload.columns)),
      ).to.be.true;
    });
  });

  describe("UPDATE_COLUMN_FILTER", () => {
    it("should set filter in state", () => {
      const result = table(initialState, {
        type: UPDATE_COLUMN_FILTER,
        columnFilter: "test",
        datasetVersion: "1234",
      });
      expect(result.getIn(["tableData", "1234", "columnFilter"])).to.equal(
        "test",
      );
    });
  });

  describe("INIT_EXPLORE_JOB_PROGRESS", () => {
    it("should set initial job Progress in state", () => {
      const result = table(initialState, {
        type: INIT_EXPLORE_JOB_PROGRESS,
        isRun: true,
      });
      const jobProgress = result.getIn(["tableData", "jobProgress"]);
      expect(jobProgress.jobId).to.be.undefined;
      expect(jobProgress.status).to.equal("STARTING");
      expect(jobProgress.isRun).to.be.equal(true);
      expect(jobProgress.startTime <= new Date().getTime()).to.be.true;
    });
  });

  describe("UPDATE_EXPLORE_JOB_PROGRESS", () => {
    it("should set initial job update in state", () => {
      const result = table(initialState, {
        type: UPDATE_EXPLORE_JOB_PROGRESS,
        jobUpdate: {
          id: "abc",
          state: "COMPLETED",
          startTime: 100,
          endTime: 200,
          outputRecords: 100,
          datasetVersion: "12345",
        },
      });
      const jobProgress = result.getIn(["tableData", "12345", "jobProgress"]);
      expect(jobProgress).to.eql({
        jobId: "abc",
        status: "COMPLETED",
        startTime: 100,
        endTime: 200,
        datasetVersion: "12345",
      });
    });
    it("should set outputRecords in state", () => {
      const result = table(initialState, {
        type: UPDATE_EXPLORE_JOB_PROGRESS,
        jobUpdate: {
          id: "abc",
          state: "COMPLETED",
          startTime: 100,
          endTime: 200,
          outputRecords: 100,
          datasetVersion: "12345",
        },
      });
      const records = result.getIn(["tableData", "12345", "outputRecords"]);
      expect(records).to.equal(100);
    });
    it("should ignore job update if there is no change", () => {
      let prevState = Immutable.fromJS({});
      prevState = prevState.setIn(["tableData", "12345", "jobProgress"], {
        jobId: "abc",
        status: "RUNNING",
        startTime: 100,
        datasetVersion: "12345",
      });
      const result = table(prevState, {
        type: UPDATE_EXPLORE_JOB_PROGRESS,
        jobUpdate: {
          id: "abc",
          state: "RUNNING",
          startTime: 100,
          datasetVersion: "12345",
        },
      });
      expect(result).to.equal(prevState);
    });
    it("should update jobProgress if there is change in payload", () => {
      let prevState = Immutable.fromJS({});
      prevState = prevState.setIn(["tableData", "12345", "jobProgress"], {
        jobId: "abc",
        status: "RUNNING",
        startTime: 100,
        datasetVersion: "12345",
      });
      const jobProgress = table(prevState, {
        type: UPDATE_EXPLORE_JOB_PROGRESS,
        jobUpdate: {
          id: "abc",
          state: "COMPLETED",
          startTime: 100,
          endTime: 200,
          outputRecords: 100,
          datasetVersion: "12345",
        },
      });
      expect(jobProgress.getIn(["tableData", "12345", "jobProgress"])).to.eql({
        jobId: "abc",
        status: "COMPLETED",
        startTime: 100,
        endTime: 200,
        datasetVersion: "12345",
      });
    });
  });

  describe("SET_EXPLORE_JOBID_IN_PROGRESS", () => {
    it("should set job id and version in jobProgress", () => {
      const result = table(initialState, {
        type: SET_EXPLORE_JOBID_IN_PROGRESS,
        jobId: "abc",
        datasetVersion: "12345",
      });
      const jobProgress = result.getIn(["tableData", "12345", "jobProgress"]);
      expect(jobProgress.jobId).to.equal("abc");
      expect(jobProgress.datasetVersion).to.equal("12345");
    });
  });

  describe("UPDATE_EXPLORE_JOB_RECORDS", () => {
    it("should set job records in state", () => {
      let result = table(initialState, {
        type: INIT_EXPLORE_JOB_PROGRESS,
      });
      let jobProgress = result.getIn(["tableData", "jobProgress"]);
      expect(jobProgress.status).to.equal("STARTING");

      result = table(initialState, {
        type: SET_EXPLORE_JOBID_IN_PROGRESS,
        datasetVersion: "12345",
      });

      result = table(result, {
        type: UPDATE_EXPLORE_JOB_RECORDS,
        recordCount: 100,
        datasetVersion: "12345",
      });
      jobProgress = result.getIn(["tableData", "12345", "jobProgress"]);
      expect(jobProgress.recordCount).to.equal(100);
    });
  });
});
