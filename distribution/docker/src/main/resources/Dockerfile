#
# Copyright (C) 2017 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM ubuntu:16.04

MAINTAINER Dremio

LABEL org.label-schema.name='dremio/dremio-oss'
LABEL org.label-schema.description='Dremio OSS container.'

RUN apt-get update && apt-get install -y openjdk-8-jdk curl

RUN addgroup --system dremio
RUN adduser --system --home /var/lib/dremio --group dremio && \
		mkdir -p /opt/dremio/log && chown -R dremio:dremio /opt/dremio && \
		mkdir /var/run/dremio && chown dremio:dremio /var/run/dremio && \
		mkdir /var/log/dremio && chown dremio:dremio /var/log/dremio && \
		chown dremio:dremio /var/lib/dremio

WORKDIR /opt/dremio

RUN curl -o dremio.tar.gz https://download.dremio.com/community-server/@project.version@/dremio-community-@project.version@.tar.gz && \
    tar xvf dremio.tar.gz -C /opt/dremio --strip-components=1 && \
    rm dremio.tar.gz

WORKDIR /opt/dremio

USER dremio

ENV DREMIO_HOME /opt/dremio

ENTRYPOINT ["bin/dremio"]
CMD ["run"]
