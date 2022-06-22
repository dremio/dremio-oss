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
import { COMMIT_TYPE } from "@app/constants/nessie";
import { expect } from "chai";
import {
  getNessieReferencePayload,
  getProjectIdFromUrl,
  getReferenceListForTransform,
  getRefQueryParams,
  getTypeAndValue,
} from "./nessieUtils";
import {
  empty,
  nessieState,
  noRef,
  nullStates,
} from "./nessieUtils-spec/getNessieReferencePayload";

describe("nessieUtils", () => {
  describe("getTypeAndValue", () => {
    it("empty state", () => {
      expect(getTypeAndValue(null)).to.equal(null);
      expect(getTypeAndValue()).to.equal(null);
    });

    it("returns branch", () => {
      expect(getTypeAndValue(nessieState["ref/dataplane"])).to.deep.equal({
        type: nessieState["ref/dataplane"].reference!.type,
        value: nessieState["ref/dataplane"].reference!.name,
      });
    });

    it("returns tag", () => {
      expect(getTypeAndValue(nessieState["ref/ref/dataplane3"])).to.deep.equal({
        type: nessieState["ref/ref/dataplane3"].reference!.type,
        value: nessieState["ref/ref/dataplane3"].reference!.name,
      });
    });

    it("returns commit", () => {
      expect(getTypeAndValue(nessieState["ref/dataplane2"])).to.deep.equal({
        type: COMMIT_TYPE,
        value: nessieState["ref/dataplane2"].hash,
      });
    });
  });

  describe("getNessieReferencePayload", () => {
    it("empty state", () => {
      expect(getNessieReferencePayload(undefined)).to.deep.equal({});
      expect(getNessieReferencePayload(null)).to.deep.equal({});
      expect(getNessieReferencePayload({})).to.deep.equal({});
      expect(getNessieReferencePayload(empty)).to.deep.equal({});
      expect(getNessieReferencePayload(noRef)).to.deep.equal({});
      expect(getNessieReferencePayload(nullStates as any)).to.deep.equal({});
    });

    it("populated state", () => {
      expect(getNessieReferencePayload(nessieState)).to.deep.equal({
        dataplane: {
          type: nessieState["ref/dataplane"].reference!.type,
          value: nessieState["ref/dataplane"].reference!.name,
        },
        dataplane2: {
          type: COMMIT_TYPE,
          value: nessieState["ref/dataplane2"].hash,
        },
        "ref/dataplane3": {
          type: nessieState["ref/ref/dataplane3"].reference!.type,
          value: nessieState["ref/ref/dataplane3"].reference!.name,
        },
      });
    });
  });

  describe("getRefQueryParams", () => {
    it("empty state", () => {
      expect(getRefQueryParams({}, "")).to.deep.equal({});
    });

    it("returns branch", () => {
      expect(getRefQueryParams(nessieState, "ref/dataplane")).to.deep.equal({
        refType: nessieState["ref/dataplane"].reference!.type,
        refValue: nessieState["ref/dataplane"].reference!.name,
      });
    });

    it("returns tag", () => {
      expect(
        getRefQueryParams(nessieState, "ref/ref/dataplane3")
      ).to.deep.equal({
        refType: nessieState["ref/ref/dataplane3"].reference!.type,
        refValue: nessieState["ref/ref/dataplane3"].reference!.name,
      });
    });

    it("returns commit", () => {
      expect(getRefQueryParams(nessieState, "ref/dataplane2")).to.deep.equal({
        refType: COMMIT_TYPE,
        refValue: nessieState["ref/dataplane2"].hash,
      });
    });
  });

  describe("getReferenceListForTransform", () => {
    it("empty state", () => {
      expect(getReferenceListForTransform(undefined)).to.deep.equal([]);
      expect(getReferenceListForTransform(null)).to.deep.equal([]);
    });

    it("populated state", () => {
      expect(
        getReferenceListForTransform(getNessieReferencePayload(nessieState))
      ).to.deep.equal([
        {
          reference: {
            type: "BRANCH",
            value: "test3",
          },
          sourceName: "dataplane",
        },
        {
          reference: {
            type: "COMMIT",
            value:
              "2867b05cd832839c9536aa58b2152bf541a00099b48244479e95fbb83ee5a228",
          },
          sourceName: "dataplane2",
        },
        {
          reference: {
            type: "TAG",
            value: "aaaaa",
          },
          sourceName: "ref/dataplane3",
        },
      ]);
    });
  });

  describe("getProjectIdFromUrl", () => {
    it("empty state", () => {
      expect(getProjectIdFromUrl(undefined)).to.equal("");
      expect(getProjectIdFromUrl(null)).to.equal("");
      expect(getProjectIdFromUrl("")).to.equal("");
    });

    it("non-string values", () => {
      expect(getProjectIdFromUrl(123)).to.equal("");
    });

    it("correctly formed url state", () => {
      expect(
        getProjectIdFromUrl(
          "http://app.test1.dremio.site/v1/projects/test-guid"
        )
      ).to.equal("test-guid");
      expect(
        getProjectIdFromUrl("http://localhost:3005/v1/projects/test-guid")
      ).to.equal("test-guid");
    });
  });
});
