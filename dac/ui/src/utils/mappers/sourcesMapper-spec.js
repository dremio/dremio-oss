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
import sourcesMapper from "./sourcesMapper";

describe("newSource", () => {
  const sourceType = "S3";
  const hostList = [
    { id: "a1", hostname: "a1", port: "1111" },
    { id: "a2", hostname: "a2", port: "2222" },
  ];
  let data, expected;

  beforeEach(() => {
    data = {};
    expected = { config: {}, type: "S3" };
  });

  it("should work w/o data.config", () => {
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result).to.eql(expected);
  });

  it("should set hostlist eliminating ids", () => {
    data.config = { hostList };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config.hostList).to.eql([
      { hostname: "a1", port: "1111" },
      { hostname: "a2", port: "2222" },
    ]);
  });

  it("should set propertylist eliminating ids", () => {
    data.config = { propertyList: hostList };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config.propertyList).to.eql([
      { hostname: "a1", port: "1111" },
      { hostname: "a2", port: "2222" },
    ]);
  });

  it("should set auth timeout as number", () => {
    data.config = { authenticationTimeoutMillis: "1572978773448" };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config.authenticationTimeoutMillis).to.equal(1572978773448);
  });

  it("should set partition size as number", () => {
    data.config = { subpartitionSize: "123" };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config.subpartitionSize).to.equal(123);
  });

  it("should clear secret for master auth", () => {
    data.config = {
      authenticationType: "MASTER",
      secretResourceUrl: "http://",
    };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config).to.eql({
      authenticationType: "MASTER",
      secretResourceUrl: "",
    });
  });

  it("should set auth type to MASTER if secret is set", () => {
    data.config = {
      authenticationType: "SECRET",
      secretResourceUrl: "http://",
    };
    const result = sourcesMapper.newSource(sourceType, data);
    expect(result.config).to.eql({
      authenticationType: "MASTER",
      secretResourceUrl: "http://",
    });
  });
});
