package org.elasticsearch.search.aggregations.metrics.percentiles;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkState;

public class PercentilesHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        // TODO figure out some way to reconstruct the t-digest, or send PR with option to serialize the t-digest in the response.
        final Map<String, Number> values = nodeMapValue(map.get("values"), String.class, Number.class);
        final ImmutableList.Builder<Percentile> builder = ImmutableList.builder();
        for (Map.Entry<String, Number> entry : values.entrySet()) {
            final double percent = Double.parseDouble(entry.getKey());
            final double value = entry.getValue().doubleValue();
            builder.add(new InternalPercentile(percent, value));
        }
        return new ApproximatePercentilesImpl(name, builder.build());
    }

    /**
     * if the input percentiles contain what you're looking for,
     * this will give it back to you, if they don't, we'll approximate
     * it. For best results, request from ES exactly the percentiles you want to know.
     */
    @VisibleForTesting
    static class ApproximatePercentilesImpl extends AbstractInternalAggregation implements Percentiles {

        private final String name;
        private final TreeMap<Double, Double> map;
        private final ImmutableList<Percentile> percentiles;

        ApproximatePercentilesImpl(final String name, final ImmutableList<Percentile> percentiles) {
            super(name);
            this.name = name;
            checkState(!percentiles.isEmpty());
            TreeMap<Double, Double> map = new TreeMap<Double, Double>();
            for (Percentile p : percentiles) {
                map.put(p.getPercent(), p.getValue());
            }
            if (!map.containsKey(0.0)) {
                map.put(0.0, 0.0);
            }
            if (!map.containsKey(100.0)) {
                map.put(100.0, map.floorEntry(100.0).getValue());
            }
            this.map = map;
            this.percentiles = percentiles;
        }

        // Potentially a very rough approximation. See the TODO above.
        @Override public double percentile(final double percent) {
            checkState(percent >= 0);
            checkState(percent <= 100);
            if (map.containsKey(percent)) {
                return map.get(percent);
            } else {
                return linearApproximation(percent);
            }
        }

        private double linearApproximation(final double percent) {
            final Map.Entry<Double, Double> floor = map.floorEntry(percent);
            final Map.Entry<Double, Double> ceiling = map.ceilingEntry(percent);

            final double floorToCeiling = ceiling.getKey() - floor.getKey();
            final double floorToPercent = percent - floor.getKey();
            final double interpolationRatio = floorToPercent / floorToCeiling;

            final double fValueToCValue = ceiling.getValue() - floor.getValue();
            final double scaled = fValueToCValue * interpolationRatio;
            final double shifted = scaled + floor.getValue();

            return shifted;
        }

        @Override public Iterator<Percentile> iterator() {
            return percentiles.iterator();
        }

    }
}
