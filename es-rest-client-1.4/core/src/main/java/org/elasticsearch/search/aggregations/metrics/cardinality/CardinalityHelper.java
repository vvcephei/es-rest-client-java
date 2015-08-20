package org.elasticsearch.search.aggregations.metrics.cardinality;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class CardinalityHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final long value = nodeLongValue(map.get("value"));
        return new ComputedCardinality(name, value);
    }

    private final static class ComputedCardinality extends AbstractInternalAggregation implements Cardinality {

        private long value;

        public ComputedCardinality(final String name, final long value) {
            super(name);
            this.value = value;
        }

        @Override public long getValue() {
            return value;
        }
    }
}
