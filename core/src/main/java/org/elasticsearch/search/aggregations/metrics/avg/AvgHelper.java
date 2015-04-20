package org.elasticsearch.search.aggregations.metrics.avg;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;

public class AvgHelper {
    public static InternalAvg fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final double value = nodeDoubleValue(map.get("value"));
        return new ComputedAvg(name, value);
    }

    private final static class ComputedAvg extends InternalAvg {
        private final String name;
        private double value;

        public ComputedAvg(final String name, final double value) {
            super();
            this.name = name;
            this.value = value;
        }

        public ComputedAvg(final String name, final double sum, final long count) {
            throw new RuntimeException("not implemented");
        }

        @Override public String getName() {
            return name;
        }

        @Override public double value() {
            return value;
        }

        @Override public double getValue() {
            return value;
        }

        @Override public Type type() {
            return super.type();
        }

        @Override public InternalAvg reduce(final ReduceContext reduceContext) {
            throw new RuntimeException("not implemented");
        }

        @Override public void readFrom(final StreamInput in) throws IOException {
            throw new RuntimeException("not implemented");
        }

        @Override public void writeTo(final StreamOutput out) throws IOException {
            throw new RuntimeException("not implemented");
        }
    }
}
