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
import jobsUtils from "./jobsUtils";

describe("jobsUtils", () => {
  describe("getFormattedRecords check", () => {
    it("should return simple number if input < 1000", () => {
      expect(jobsUtils.getFormattedRecords(990)).to.be.eql(990);
      expect(jobsUtils.getFormattedRecords(880)).to.be.eql(880);
      expect(jobsUtils.getFormattedRecords(1)).to.be.eql(1);
      expect(jobsUtils.getFormattedRecords(0)).to.be.eql(0);
      expect(jobsUtils.getFormattedRecords(12)).to.be.eql(12);
    });

    it("should return ~ number of thousand if input < 1000 000 but bigger 1000", () => {
      expect(jobsUtils.getFormattedRecords(1000)).to.be.eql("1,000");
      expect(jobsUtils.getFormattedRecords(2000)).to.be.eql("2,000");
      expect(jobsUtils.getFormattedRecords(995500)).to.be.eql("995,500");
      expect(jobsUtils.getFormattedRecords(995600)).to.be.eql("995,600");
      expect(jobsUtils.getFormattedRecords(995400)).to.be.eql("995,400");
      expect(jobsUtils.getFormattedRecords(1200)).to.be.eql("1,200");
      expect(jobsUtils.getFormattedRecords(34400)).to.be.eql("34,400");
    });

    it("should return ~ number of million if bigger 1000 000", () => {
      expect(jobsUtils.getFormattedRecords(1000000)).to.be.eql("1,000,000");
      expect(jobsUtils.getFormattedRecords(12000000)).to.be.eql("12,000,000");
      expect(jobsUtils.getFormattedRecords(99500000)).to.be.eql("99,500,000");
      expect(jobsUtils.getFormattedRecords(99400000)).to.be.eql("99,400,000");
      expect(jobsUtils.getFormattedRecords(99000000)).to.be.eql("99,000,000");
    });

    it("should return empty string if we have invalide input", () => {
      expect(jobsUtils.getFormattedRecords(NaN)).to.be.eql("");
      expect(jobsUtils.getFormattedRecords()).to.be.eql("");
      expect(jobsUtils.getFormattedRecords("blabla")).to.be.eql("");
    });
  });

  describe("getReflectionsByRelationship", () => {
    it("no relationships recorded (failed planning, pre-1.3, etc)", () => {
      expect(
        jobsUtils.getReflectionsByRelationship(new Immutable.Map())
      ).to.be.eql({});
    });
    it("", () => {
      const jobDetails = Immutable.fromJS({
        acceleration: {
          reflectionRelationships: [
            {
              relationship: "CONSIDERED",
              "test-extra": 0,
            },
            {
              relationship: "CHOSEN",
              "test-extra": 1,
            },
            {
              relationship: "MATCHED",
              "test-extra": 2,
            },
            {
              relationship: "CONSIDERED",
              "test-extra": 3,
            },
            {
              relationship: "CHOSEN",
              "test-extra": 4,
            },
            {
              relationship: "MATCHED",
              "test-extra": 5,
            },
          ],
        },
      });

      expect(jobsUtils.getReflectionsByRelationship(jobDetails)).to.be.eql({
        CONSIDERED: [
          {
            relationship: "CONSIDERED",
            "test-extra": 0,
          },
          {
            relationship: "CONSIDERED",
            "test-extra": 3,
          },
        ],
        CHOSEN: [
          {
            relationship: "CHOSEN",
            "test-extra": 1,
          },
          {
            relationship: "CHOSEN",
            "test-extra": 4,
          },
        ],
        MATCHED: [
          {
            relationship: "MATCHED",
            "test-extra": 2,
          },
          {
            relationship: "MATCHED",
            "test-extra": 5,
          },
        ],
      });
    });
  });

  describe("getFormattedNumber", () => {
    it("should return number without units if input < 1000", () => {
      expect(jobsUtils.getFormattedNumber(990)).to.be.eql("990");
      expect(jobsUtils.getFormattedNumber(880)).to.be.eql("880");
      expect(jobsUtils.getFormattedNumber(1)).to.be.eql("1");
    });

    it("should return ~ number with unit K if input < 1000 000 but bigger 1000", () => {
      expect(jobsUtils.getFormattedNumber(1000)).to.be.eql("1K");
      expect(jobsUtils.getFormattedNumber(545100)).to.be.eql("545.1K");
    });

    it("should return ~ number with unit M if if bigger 1000 000", () => {
      expect(jobsUtils.getFormattedNumber(907525561)).to.be.eql("907.5M");
      expect(jobsUtils.getFormattedNumber(981100018)).to.be.eql("981.1M");
    });

    it("should return ~ number with unit B if if bigger 1000 000 000", () => {
      expect(jobsUtils.getFormattedNumber(4289559013)).to.be.eql("4.3B");
      expect(jobsUtils.getFormattedNumber(5289559013)).to.be.eql("5.3B");
    });

    it("should return ~ number with unit T if if bigger 1000 000 000 000", () => {
      expect(jobsUtils.getFormattedNumber(1000000000000)).to.be.eql("1T");
      expect(jobsUtils.getFormattedNumber(572191587726634)).to.be.eql("572.2T");
    });

    it("should return ~ number with unit P if if bigger 1000 000 000 000 000", () => {
      expect(jobsUtils.getFormattedNumber(1000000000000000)).to.be.eql("1P");
      expect(jobsUtils.getFormattedNumber(672191587726634123)).to.be.eql(
        "672.2P"
      );
    });

    it("should return ~ number with unit E if if bigger 1000 000 000 000 000 000", () => {
      expect(jobsUtils.getFormattedNumber(1000000000000000000)).to.be.eql("1E");
      expect(jobsUtils.getFormattedNumber(772191587726634123000)).to.be.eql(
        "772.2E"
      );
    });
  });
});
