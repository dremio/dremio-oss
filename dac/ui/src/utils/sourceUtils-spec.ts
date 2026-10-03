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
import { expect } from "chai";
import * as sinon from "sinon";
import sourcesMapper from "#oss/utils/mappers/sourcesMapper";
import {
  POLARIS_PRESET_ID,
  SOURCE_PRESETS,
  addSourcePresetTiles,
  getSourcePreset,
  getSourcePresetInitialValues,
  getSourceTypeLoadErrorMessage,
  isSensitivePropertyKey,
  validateSourcePropertyLists,
  withSourcePropertyListValidation,
} from "./sourceUtils";

describe("sourceUtils", () => {
  describe("source presets", () => {
    it("every preset is based on an existing backend type", () => {
      SOURCE_PRESETS.forEach((preset) => {
        expect(preset.sourceType).to.equal("RESTCATALOG");
      });
    });

    it("getSourcePreset returns undefined for unknown or empty ids", () => {
      expect(getSourcePreset(undefined)).to.be.undefined;
      expect(getSourcePreset(null)).to.be.undefined;
      expect(getSourcePreset("NOPE")).to.be.undefined;
      expect(getSourcePreset(POLARIS_PRESET_ID)!.label).to.equal(
        "Apache Polaris OSS",
      );
    });

    describe("addSourcePresetTiles", () => {
      const restCatalog = {
        sourceType: "RESTCATALOG",
        label: "Iceberg REST Catalog",
        icon: "icon.svg",
      };
      const s3 = { sourceType: "S3", label: "Amazon S3" };

      it("adds a Polaris tile next to the generic REST catalog tile", () => {
        const result = addSourcePresetTiles([s3, restCatalog]);
        expect(result).to.have.length(3);
        expect(result[0]).to.equal(s3);
        expect(result[1]).to.equal(restCatalog);
        expect(result[2]).to.deep.equal({
          sourceType: "RESTCATALOG",
          label: "Apache Polaris OSS",
          icon: "icon.svg",
          presetId: POLARIS_PRESET_ID,
        });
      });

      it("does not add the preset when RESTCATALOG is missing or disabled", () => {
        const list = [s3];
        expect(addSourcePresetTiles(list)).to.equal(list);
        expect(
          addSourcePresetTiles([s3, { ...restCatalog, disabled: true }]),
        ).to.have.length(2);
      });

      it("is idempotent", () => {
        const once = addSourcePresetTiles([restCatalog]);
        expect(addSourcePresetTiles(once)).to.have.length(2);
      });

      it("handles a missing list", () => {
        expect(addSourcePresetTiles(undefined as any)).to.be.undefined;
      });
    });

    describe("getSourcePresetInitialValues", () => {
      it("returns undefined without a preset", () => {
        expect(getSourcePresetInitialValues(undefined)).to.be.undefined;
        expect(getSourcePresetInitialValues("NOPE")).to.be.undefined;
      });

      it("pre-fills the Polaris properties", () => {
        const values = getSourcePresetInitialValues(POLARIS_PRESET_ID)!;
        const config = values.config as Record<string, any>;
        expect(config.isUsingVendedCredentials).to.equal(false);
        expect(
          config.propertyList.map(({ name, value }: any) => ({ name, value })),
        ).to.deep.equal([
          { name: "warehouse", value: "" },
          { name: "scope", value: "PRINCIPAL_ROLE:ALL" },
        ]);
        expect(
          config.secretPropertyList.map(({ name, value }: any) => ({
            name,
            value,
          })),
        ).to.deep.equal([{ name: "credential", value: "" }]);
      });

      it("gives every row a unique id and does not share state between calls", () => {
        const first = getSourcePresetInitialValues(POLARIS_PRESET_ID)!
          .config as Record<string, any>;
        const second = getSourcePresetInitialValues(POLARIS_PRESET_ID)!
          .config as Record<string, any>;
        const ids = [
          ...first.propertyList,
          ...first.secretPropertyList,
          ...second.propertyList,
        ].map((row: any) => row.id);
        expect(new Set(ids).size).to.equal(ids.length);
        first.propertyList[0].value = "changed";
        expect(second.propertyList[0].value).to.equal("");
        expect(SOURCE_PRESETS[0].propertyList![0].value).to.equal("");
      });

      it("is saved as a RESTCATALOG source without row ids", () => {
        const values = getSourcePresetInitialValues(POLARIS_PRESET_ID)!;
        const source = sourcesMapper.newSource("RESTCATALOG", {
          name: "polaris",
          ...values,
        }) as any;
        expect(source.type).to.equal("RESTCATALOG");
        expect(source.config.propertyList[1]).to.deep.equal({
          name: "scope",
          value: "PRINCIPAL_ROLE:ALL",
        });
        expect(source.config.secretPropertyList[0]).to.not.have.property("id");
      });
    });
  });

  describe("isSensitivePropertyKey", () => {
    it("matches secret keys case-insensitively", () => {
      expect(isSensitivePropertyKey("credential")).to.be.true;
      expect(isSensitivePropertyKey(" Token ")).to.be.true;
      expect(isSensitivePropertyKey("fs.s3a.secret.key")).to.be.true;
      expect(isSensitivePropertyKey("S3.Secret-Access-Key")).to.be.true;
      expect(isSensitivePropertyKey("header.Authorization")).to.be.true;
      expect(isSensitivePropertyKey("fs.s3a.access.key")).to.be.true;
      expect(isSensitivePropertyKey("s3.access-key-id")).to.be.true;
    });

    it("matches the same patterns as the backend", () => {
      expect(isSensitivePropertyKey("fs.s3a.bucket.x.secret.key")).to.be.true;
      expect(isSensitivePropertyKey("fs.azure.account.key.acct")).to.be.true;
      expect(isSensitivePropertyKey("gcs.oauth2.token")).to.be.true;
      expect(isSensitivePropertyKey("adls.sas-token.acct")).to.be.true;
      expect(isSensitivePropertyKey("my-client-secret")).to.be.true;
      expect(isSensitivePropertyKey("db.Password")).to.be.true;
      expect(isSensitivePropertyKey("gcs.private-key")).to.be.true;
      expect(isSensitivePropertyKey("header.X-Api-Token")).to.be.true;
      expect(isSensitivePropertyKey("refresh_token")).to.be.true;
    });

    it("does not match regular keys", () => {
      expect(isSensitivePropertyKey("warehouse")).to.be.false;
      expect(isSensitivePropertyKey("scope")).to.be.false;
      expect(isSensitivePropertyKey("fs.s3a.endpoint")).to.be.false;
      expect(isSensitivePropertyKey("oauth2-server-uri")).to.be.false;
      expect(isSensitivePropertyKey("token-expires-in-ms")).to.be.false;
      expect(isSensitivePropertyKey("fs.s3a.path.style.access")).to.be.false;
      expect(isSensitivePropertyKey(undefined)).to.be.false;
      expect(isSensitivePropertyKey("")).to.be.false;
    });
  });

  describe("validateSourcePropertyLists", () => {
    it("ignores other source types and missing config", () => {
      const config = { propertyList: [{ name: "", value: "" }] };
      expect(validateSourcePropertyLists("S3", { config })).to.deep.equal({});
      expect(validateSourcePropertyLists("RESTCATALOG", {})).to.deep.equal({});
      expect(
        validateSourcePropertyLists("RESTCATALOG", undefined),
      ).to.deep.equal({});
    });

    it("accepts complete rows and masked secret values", () => {
      expect(
        validateSourcePropertyLists("RESTCATALOG", {
          config: {
            propertyList: [
              { id: "1", name: "warehouse", value: "polaris_demo" },
              { id: "2", name: "scope", value: "PRINCIPAL_ROLE:ALL" },
            ],
            secretPropertyList: [
              { name: "credential", value: "$DREMIO_EXISTING_VALUE$" },
            ],
          },
        }),
      ).to.deep.equal({});
    });

    it("blocks the empty rows of the Polaris preset", () => {
      const values = getSourcePresetInitialValues(POLARIS_PRESET_ID);
      expect(validateSourcePropertyLists("RESTCATALOG", values)).to.deep.equal({
        config: {
          propertyList: [{ value: "Value is required." }, {}],
          secretPropertyList: [{ value: "Value is required." }],
        },
      });
    });

    it("rejects secret keys in propertyList but not in secretPropertyList", () => {
      const errors = validateSourcePropertyLists("RESTCATALOG", {
        config: {
          propertyList: [
            { name: "warehouse", value: "w" },
            { name: "Credential", value: "x" },
          ],
          secretPropertyList: [{ name: "credential", value: "x" }],
        },
      }) as any;
      expect(errors.config.secretPropertyList).to.be.undefined;
      expect(errors.config.propertyList[0]).to.deep.equal({});
      expect(errors.config.propertyList[1].name).to.contain(
        "Catalog Credentials",
      );
      expect(errors.config.propertyList[1].name).to.not.contain('"x"');
    });

    it("blocks new sensitive keys but keeps the ones already saved (edit)", () => {
      const initialPropertyList = [
        { name: "warehouse", value: "w" },
        { name: "Credential", value: "x" },
      ];
      const errors = validateSourcePropertyLists(
        "RESTCATALOG",
        {
          config: {
            propertyList: [
              { name: "warehouse", value: "w" },
              { name: "credential", value: "x" },
              { name: "fs.s3a.secret.key", value: "y" },
            ],
          },
        },
        { initialPropertyList },
      ) as any;
      expect(errors.config.propertyList[0]).to.deep.equal({});
      expect(errors.config.propertyList[1]).to.deep.equal({});
      expect(errors.config.propertyList[2].name).to.contain(
        "Catalog Credentials",
      );

      expect(
        validateSourcePropertyLists(
          "RESTCATALOG",
          { config: { propertyList: initialPropertyList } },
          { initialPropertyList },
        ),
      ).to.deep.equal({});
    });

    it("requires a name", () => {
      const errors = validateSourcePropertyLists("RESTCATALOG", {
        config: { propertyList: [{ name: "  ", value: "v" }, null] },
      }) as any;
      expect(errors.config.propertyList).to.deep.equal([
        { name: "Name is required." },
        { name: "Name is required.", value: "Value is required." },
      ]);
    });
  });

  describe("withSourcePropertyListValidation", () => {
    it("merges with the base validator", () => {
      const base = sinon.stub().returns({ name: "Name is required" });
      const validate = withSourcePropertyListValidation(base, "RESTCATALOG");
      const values = { config: { propertyList: [{ name: "a", value: "" }] } };
      expect(validate(values)).to.deep.equal({
        name: "Name is required",
        config: { propertyList: [{ value: "Value is required." }] },
      });
      expect(base.calledWith(values)).to.be.true;
    });

    it("passes the edit options through", () => {
      const validate = withSourcePropertyListValidation(
        undefined,
        "RESTCATALOG",
        { initialPropertyList: [{ name: "token", value: "t" }] },
      );
      expect(
        validate({ config: { propertyList: [{ name: "token", value: "t" }] } }),
      ).to.deep.equal({});
    });

    it("keeps the base result for other source types", () => {
      const validate = withSourcePropertyListValidation(
        () => ({ config: { hostname: "required" } }),
        "S3",
      );
      expect(
        validate({ config: { propertyList: [{ name: "", value: "" }] } }),
      ).to.deep.equal({ config: { hostname: "required" } });
      expect(
        withSourcePropertyListValidation(undefined, "S3")({}),
      ).to.deep.equal({});
    });
  });

  describe("getSourceTypeLoadErrorMessage", () => {
    it("reads errorMessage from a Response", async () => {
      const response = {
        json: () => Promise.resolve({ errorMessage: "Unknown source type" }),
      };
      expect(await getSourceTypeLoadErrorMessage(response)).to.equal(
        "Unknown source type",
      );
    });

    it("returns undefined for errors that are not a Response", async () => {
      expect(await getSourceTypeLoadErrorMessage(new TypeError("boom"))).to.be
        .undefined;
      expect(await getSourceTypeLoadErrorMessage(undefined)).to.be.undefined;
    });

    it("returns undefined when the body is not JSON", async () => {
      const response = { json: () => Promise.reject(new SyntaxError("bad")) };
      expect(await getSourceTypeLoadErrorMessage(response)).to.be.undefined;
      const empty = { json: () => Promise.resolve({}) };
      expect(await getSourceTypeLoadErrorMessage(empty)).to.be.undefined;
    });
  });
});
