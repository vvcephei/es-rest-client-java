package org.elasticsearch.search.aggregations.metrics.min;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;

public class MinHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final double value = nodeDoubleValue(map.get("value"));
        return new MinImpl(name, value);
    }

    private static final class MinImpl extends AbstractInternalAggregation implements Min {

        private final double value;

        public MinImpl(final String name, final double value) {
            super(name);
            this.value = value;
        }

        @Override public double getValue() {
            return value;
        }
    }
}
