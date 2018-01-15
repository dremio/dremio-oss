<#--

    Copyright (C) 2017 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

<#include "*/generic.ftl">
<#macro page_head>
<script src="/static/js/d3.v3.js"></script>
<script src="/static/js/dagre-d3.min.js"></script>
<script src="/static/js/graph.js"></script>
<script>
    var globalconfig = {
        "queryid" : "${model.getQueryId()}",
        "operators" : ${model.getOperatorsJSON()?no_esc},
        "planText": "${model.getPlanText()}",
        "fragmentProfileSize": ${model.getFragmentProfilesSize()}
    };
</script>
</#macro>

<#macro page_body>
  <h3>Query and Planning</h3>
  <ul id="query-tabs" class="nav nav-tabs" role="tablist">
    <li><a href="#query-query" role="tab" data-toggle="tab">Query</a></li>
    <li><a href="#query-visual" role="tab" data-toggle="tab">Visualized Plan</a></li>
    <li><a href="#query-plan" role="tab" data-toggle="tab">Planning</a></li>
    <li><a href="#query-acceleration" role="tab" data-toggle="tab">Acceleration</a></li>
    <#if model.hasError() ><li><a href="#query-error" role="tab" data-toggle="tab">Error</a></li></#if>
  </ul>
  <div id="query-content" class="tab-content">
    <div id="query-query" class="tab-pane">
      <p><pre>${model.profile.query}</pre></p>
    </div>
    <div id="query-physical" class="tab-pane">
      <p><pre>${model.profile.plan}</pre></p>
    </div>
    <div id="query-visual" class="tab-pane">
      <svg id="query-visual-canvas" class="center-block"></svg>
    </div>
    <div id="query-acceleration" class="tab-pane">
      <h3>Reflection Outcome</h3>
      <#if model.profile.hasAccelerationProfile() && model.profile.getAccelerationProfile().getAccelerated()>
        <p>Query was accelerated</p>
      <#else>
        <p>Query was NOT accelerated</p>
      </#if>
      <#if model.profile.hasAccelerationProfile() && model.profile.getAccelerationProfile().getLayoutProfilesCount() != 0>
        <ul>
          <#list model.profile.getAccelerationProfile().getLayoutProfilesList() as layout>
            <#if layout.name?? && layout.name?trim?has_content >
              <#assign layoutName = layout.name>
            <#else>
              <#assign layoutName = layout.layoutId>
            </#if>
            <li>${layoutName} (<#if layout.displayColumnsList?has_content>raw<#else>agg</#if>): considered<#if layout.numSubstitutions != 0>, matched<#if layout.numUsed != 0>, chosen<#else>, not chosen</#if><#else>, not matched</#if>.</li>
          </#list>
        </ul>
      </#if>

      <#if model.profile.hasAccelerationProfile()>
        <p>
        Time To Find Reflections:   ${model.getProfile().getAccelerationProfile().getMillisTakenGettingMaterializations()} ms
        <br>
        Time To Canonicalize:   ${model.getProfile().getAccelerationProfile().getMillisTakenNormalizing()} ms
        <br>
        Time To Match:   ${model.getProfile().getAccelerationProfile().getMillisTakenSubstituting()} ms
        </p>
      <#else>
        <p>
        Time To Find Reflections:   --
        <br>
        Time To Canonicalize:   --
        <br>
        Time To Match:   --
        </p>
      </#if>
      <h3>Canonicalized User Query Alternatives</h3>
      <#if model.profile.hasAccelerationProfile()>
        <#list model.profile.getAccelerationProfile().getNormalizedQueryPlansList() as normalizedPlan>
          <#if normalizedPlan?has_content >
          <p><pre>${normalizedPlan}</pre></p>
          </#if>
        </#list>
      </#if>
      <h3>Reflection Details</h3>
        <#if model.profile.hasAccelerationProfile() && model.profile.getAccelerationProfile().getLayoutProfilesCount() != 0>
          <#list model.profile.getAccelerationProfile().getLayoutProfilesList() as layout>

          <#if layout.name?? && layout.name?trim?has_content >
          <#assign layoutName = layout.name>
          <#else>
          <#assign layoutName = layout.layoutId>
          </#if>
          <h4>Reflection Definition: ${layoutName}</h4>
          <p>
          Matched:   ${layout.getNumSubstitutions()}, Chosen:  ${layout.getNumUsed()}, Match Latency:   ${layout.getMillisTakenSubstituting()} ms<br>
          </p>
          <p>
          Reflection Id: ${layout.getLayoutId()}, Materialization Id: ${layout.getMaterializationId()}<br>
          Expiration:   ${layout.materializationExpirationTimestamp?number_to_datetime?iso_utc}<br>
          <#if model.accelerationDetails?? && model.accelerationDetails.hasRelationship(layout.layoutId) >
          Dataset: ${model.accelerationDetails.getReflectionDatasetPath(layout.layoutId)}<br>
          </#if>
          <#if layout.dimensionsList?has_content >
          Dimensions:
            <#list layout.getDimensionsList() as dim>
              ${dim},
            </#list>
          <br />
          </#if>

          <#if layout.measuresList?has_content >
          Measures:
            <#list layout.getMeasuresList() as measures>
              ${measures},
            </#list>
          <br />
          </#if>

          <#if layout.displayColumnsList?has_content >
          Display:
            <#list layout.getDisplayColumnsList() as display>
              ${display},
            </#list>
          <br />
          </#if>

          <#if layout.sortedColumnsList?has_content >
          Sorted:
            <#list layout.getSortedColumnsList() as sorted>
              ${sorted},
            </#list>
          <br />
          </#if>

          <#if layout.partitionedColumnsList?has_content >
          Partitioned:
            <#list layout.getPartitionedColumnsList() as partitioned>
              ${partitioned},
            </#list>
          <br />
          </#if>

          <#if layout.distributionColumnsList?has_content >
          Distributed:
            <#list layout.getDistributionColumnsList() as dist>
              ${dist},
            </#list>
          <br />
          </#if>
          </p>

          <#if layout.plan?has_content >
          <p>Reflection Plan:
            <pre>${layout.getPlan()}</pre>
          </p>
          </#if>

          <p>Canonicalized Reflection Plans:
            <#list layout.getNormalizedPlansList() as planNorm>
            <#if planNorm?has_content >
              <p><pre>${planNorm}</pre></p>
            </#if>
            </#list>
          </p>

          <p>Replacement Plans:
            <#list layout.getSubstitutionsList() as substitution>
              <#if substitution?has_content >
              <p><pre>${substitution.getPlan()}</pre></p>
              </#if>
            </#list>
          </p>

          <p>Best Cost Replacement Plan:
            <#assign optimizedPlan = layout.getOptimizedPlan()>
            <#if optimizedPlan?has_content>
              <p><pre>${optimizedPlan}</pre></p>
            </#if>
          </p>
          </#list>
          <hr />
        <#else>
          <p>No Reflections Were Applicable.</p>
        </#if>
    </div>
    <div id="query-plan" class="tab-pane">
      <#if model.profile.planPhasesCount != 0>
        <#list model.profile.planPhasesList as planPhase>
          <p>
          ${planPhase.getPhaseName()} (${planPhase.getDurationMillis()} ms)<br />
          <#if planPhase.plan?has_content><p><pre>${planPhase.plan}</pre></p></#if>
          <#if planPhase.plannerDump?has_content><p><pre>${planPhase.plannerDump}</pre></p></#if>
          </p>
        </#list>
      <#else>
        <p>No planning phase information to show</p>
      </#if>
      <#if model.querySchema?has_content>
        <h3>Query Output Schema</h3>
        <p><pre>${model.querySchema}</pre></p>
      </#if>
    </div>

    <#if model.hasError() >
    <div id="query-error" class="tab-pane">
      <p>
      <pre>
      ${model.getProfile().error?trim}
      </pre>
      </p>
      <p>Failure node: ${model.getProfile().errorNode}</p>
      <p>Error ID: ${model.getProfile().errorId}</p>
      <p></p><p>Verbose:</p>
      <p><pre>
         ${model.getProfile().verboseError?trim}
      </pre></p>
    </div>
    </#if>
  </div>

  <h3>Job Summary</h3>
  <dl class="dl-horizontal info-list">
    <dt>State:</dt>
    <dd>${model.getProfile().getState().name()}</dd>
    <dt>Coordinator:</dt>
    <dd>${model.getProfile().getForeman().getAddress()}</dd>
    <dt>Threads:</dt>
    <dd>${model.getProfile().getTotalFragments()}</dd>
    <dt>Planning Time:</dt>
    <dd>${model.getPlanningTime()}</dd>
  </dl>
  <h3>Threads</h3>
  <div class="panel-group" id="fragment-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#fragment-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="fragment-overview" class="panel-collapse collapse">
        <div class="panel-body">
          ${model.getFragmentsOverview()?no_esc}
        </div>
      </div>
    </div>
    <#list model.getFragmentProfiles() as frag>
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#${frag.getId()}" class="collapsed">
            ${frag.getDisplayName()}
          </a>
        </h4>
      </div>
      <div id="${frag.getId()}" class="panel-collapse collapse">
        <div class="panel-body">
          ${frag.getContent()?no_esc}
          <div class="panel panel-default">
            <div class="panel-heading">
              <h4 class="panel-title">
                <a data-toggle="collapse" href="#${frag.getId()}-metrics" class="collapsed">
                  Phase Metrics
                </a>
              </h4>
            </div>
            <div id="${frag.getId()}-metrics" class="panel-collapse collapse">
              <div class="panel-body">
                ${frag.getMetricsTable()?no_esc}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    </#list>
  </div>

  <h3>Operators</h3>

  <div class="panel-group" id="operator-accordion">
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#operator-overview">
            Overview
          </a>
        </h4>
      </div>
      <div id="operator-overview" class="panel-collapse collapse">
        <div class="panel-body">
          ${model.getOperatorsOverview()?no_esc}
        </div>
      </div>
    </div>

    <#list model.getOperatorProfiles() as op>
    <div class="panel panel-default">
      <div class="panel-heading">
        <h4 class="panel-title">
          <a data-toggle="collapse" href="#${op.getId()}" class="collapsed">
            ${op.getDisplayName()}
          </a>
        </h4>
      </div>
      <div id="${op.getId()}" class="panel-collapse collapse">
        <div class="panel-body">
          ${op.getContent()?no_esc}
          <div class="panel panel-default">
            <div class="panel-heading">
              <h4 class="panel-title">
                <a data-toggle="collapse" href="#${op.getId()}-metrics" class="collapsed">
                  Operator Metrics
                </a>
              </h4>
            </div>
            <div id="${op.getId()}-metrics" class="panel-collapse collapse">
              <div class="panel-body">
                ${op.getMetricsTable()?no_esc}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    </#list>
  </div>
</#macro>

<@page_html/>
