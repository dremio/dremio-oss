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
/*
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
package io.airlift.tpch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.tpch.DistributionLoader.loadDistribution;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;

public class Distributions
{
    private static Distributions DEFAULT_DISTRIBUTIONS;

    public static synchronized Distributions getDefaultDistributions()
    {
        if (DEFAULT_DISTRIBUTIONS == null) {
            try {
                URL resource = Resources.getResource("dists.dss");
                checkState(resource != null, "Distribution file 'dists.dss' not found");
                DEFAULT_DISTRIBUTIONS = new Distributions(loadDistribution(Resources.asCharSource(resource, UTF_8)));
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return DEFAULT_DISTRIBUTIONS;
    }

    private final Distribution grammars;
    private final Distribution nounPhrase;
    private final Distribution verbPhrase;
    private final Distribution prepositions;
    private final Distribution nouns;
    private final Distribution verbs;
    private final Distribution articles;
    private final Distribution adjectives;
    private final Distribution adverbs;
    private final Distribution auxiliaries;
    private final Distribution terminators;
    private final Distribution orderPriorities;
    private final Distribution shipInstructions;
    private final Distribution shipModes;
    private final Distribution returnFlags;
    private final Distribution partContainers;
    private final Distribution partColors;
    private final Distribution partTypes;
    private final Distribution marketSegments;
    private final Distribution nations;
    private final Distribution regions;

    public Distributions(Map<String, Distribution> distributions)
    {
        this.grammars = getDistribution(distributions, "grammar");
        this.nounPhrase = getDistribution(distributions, "np");
        this.verbPhrase = getDistribution(distributions, "vp");
        this.prepositions = getDistribution(distributions, "prepositions");
        this.nouns = getDistribution(distributions, "nouns");
        this.verbs = getDistribution(distributions, "verbs");
        this.articles = getDistribution(distributions, "articles");
        this.adjectives = getDistribution(distributions, "adjectives");
        this.adverbs = getDistribution(distributions, "adverbs");
        this.auxiliaries = getDistribution(distributions, "auxillaries");
        this.terminators = getDistribution(distributions, "terminators");
        this.orderPriorities = getDistribution(distributions, "o_oprio");
        this.shipInstructions = getDistribution(distributions, "instruct");
        this.shipModes = getDistribution(distributions, "smode");
        this.returnFlags = getDistribution(distributions, "rflag");
        this.partContainers = getDistribution(distributions, "p_cntr");
        this.partColors = getDistribution(distributions, "colors");
        this.partTypes = getDistribution(distributions, "p_types");
        this.marketSegments = getDistribution(distributions, "msegmnt");
        this.nations = getDistribution(distributions, "nations");
        this.regions = getDistribution(distributions, "regions");
    }

    public Distribution getAdjectives()
    {
        return adjectives;
    }

    public Distribution getAdverbs()
    {
        return adverbs;
    }

    public Distribution getArticles()
    {
        return articles;
    }

    public Distribution getAuxiliaries()
    {
        return auxiliaries;
    }

    public Distribution getGrammars()
    {
        return grammars;
    }

    public Distribution getMarketSegments()
    {
        return marketSegments;
    }

    public Distribution getNations()
    {
        return nations;
    }

    public Distribution getNounPhrase()
    {
        return nounPhrase;
    }

    public Distribution getNouns()
    {
        return nouns;
    }

    public Distribution getOrderPriorities()
    {
        return orderPriorities;
    }

    public Distribution getPartColors()
    {
        return partColors;
    }

    public Distribution getPartContainers()
    {
        return partContainers;
    }

    public Distribution getPartTypes()
    {
        return partTypes;
    }

    public Distribution getPrepositions()
    {
        return prepositions;
    }

    public Distribution getRegions()
    {
        return regions;
    }

    public Distribution getReturnFlags()
    {
        return returnFlags;
    }

    public Distribution getShipInstructions()
    {
        return shipInstructions;
    }

    public Distribution getShipModes()
    {
        return shipModes;
    }

    public Distribution getTerminators()
    {
        return terminators;
    }

    public Distribution getVerbPhrase()
    {
        return verbPhrase;
    }

    public Distribution getVerbs()
    {
        return verbs;
    }

    private static Distribution getDistribution(Map<String, Distribution> distributions, String name)
    {
        Distribution distribution = distributions.get(name);
        checkArgument(distribution != null, "Distribution does not exist: %s", name);
        return distribution;
    }
}
