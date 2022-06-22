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
import { getIconByType, getUrlByType } from "./utils";

describe("NessieHomePage - utils", () => {
  it("Returns the correct URL", () => {
    expect(getUrlByType("UNKNOWN", "a.b.c")).to.equal("/namespace/a.b.c");
    expect(getUrlByType(null, "a.b.c")).to.equal("/namespace/a.b.c");
    expect(getUrlByType(null, "")).to.equal("/namespace/");
    expect(getUrlByType("ICEBERG_TABLE", "a.b.c")).to.equal("/table/a.b.c");
    expect(getUrlByType("ICEBERG_TABLE", "")).to.equal("/table/");
  });

  it("Returns the correct icon", () => {
    const namespace = { type: "Space", id: "Nessie.Namespace" };
    const iceberg = { type: "PhysicalDataset", id: "Nessie.ICEBERG_TABLE" };
    expect(getIconByType("ICEBERG_TABLE")).to.deep.equal(iceberg);
    expect(getIconByType("UNKNOWN")).to.deep.equal(namespace);
    expect(getIconByType("")).to.deep.equal(namespace);
    expect(getIconByType(null)).to.deep.equal(namespace);
    expect(getIconByType()).to.deep.equal(namespace);
  });
});
