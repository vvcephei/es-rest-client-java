package org.elasticsearch.search.aggregations.metrics.avg;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;

public class AvgHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final double value = nodeDoubleValue(map.get("value"));
        return new ComputedAvg(name, value);
    }

    private final static class ComputedAvg extends AbstractInternalAggregation implements Avg {

        private double value;

        public ComputedAvg(final String name, final double value) {
            super(name);
            this.value = value;
        }

        @Override public double getValue() {
            return value;
        }

    }
}
