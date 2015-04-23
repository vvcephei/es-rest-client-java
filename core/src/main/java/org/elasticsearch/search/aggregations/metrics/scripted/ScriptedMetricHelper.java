package org.elasticsearch.search.aggregations.metrics.scripted;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

public class ScriptedMetricHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        return new ComputedScriptedMetric(name, map.get("value"));
    }

    private final static class ComputedScriptedMetric extends AbstractInternalAggregation implements ScriptedMetric {

        private final Object aggregation;

        private ComputedScriptedMetric(final String name, final Object aggregation) {
            super(name);
            this.aggregation = aggregation;
        }

        @Override public Object aggregation() {
            return aggregation;
        }
    }
}
