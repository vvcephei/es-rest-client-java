package org.elasticsearch.search.aggregations.metrics.percentiles;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.common.annotations.VisibleForTesting;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkState;

public class PercentileRanksHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        // TODO figure out some way to reconstruct the t-digest, or send PR with option to serialize the t-digest in the response.
        final Map<String, Number> values = nodeMapValue(map.get("values"), String.class, Number.class);
        final ImmutableList.Builder<Percentile> builder = ImmutableList.builder();
        for (Map.Entry<String, Number> entry : values.entrySet()) {
            final double percent = entry.getValue().doubleValue();
            final double value = Double.parseDouble(entry.getKey());
            builder.add(new InternalPercentile(percent, value));
        }
        return new ApproximatePercentileRanksImpl(name, builder.build());
    }

    /**
     * if the input percentiles contain what you're looking for,
     * this will give it back to you, if they don't, we'll approximate
     * it. For best results, request from ES exactly the percentiles you want to know.
     */
    @VisibleForTesting
    static class ApproximatePercentileRanksImpl extends AbstractInternalAggregation implements PercentileRanks {

        private final String name;
        private final TreeMap<Double, Double> map;
        private final ImmutableList<Percentile> percents;

        ApproximatePercentileRanksImpl(final String name, final ImmutableList<Percentile> percents) {
            super(name);
            this.name = name;
            checkState(!percents.isEmpty());
            TreeMap<Double, Double> map = new TreeMap<Double, Double>();
            for (Percentile p : percents) {
                map.put(p.getValue(), p.getPercent());
            }
            this.map = map;
            this.percents = percents;
        }

        // Potentially a very rough approximation. See the TODO above.
        @Override public double percent(final double value) {
            if (map.containsKey(value)) {
                return map.get(value);
            } else {
                return linearApproximation(value);
            }
        }

        private double linearApproximation(final double value) {
            final Map.Entry<Double, Double> floor = map.floorEntry(value);
            if (floor == null) {
                return 0.0;
            }
            final Map.Entry<Double, Double> ceiling = map.ceilingEntry(value);
            if (ceiling == null){
                return floor.getValue(); // Sadly, this is our best guess.
            }

            final double floorToCeiling = ceiling.getKey() - floor.getKey();
            final double floorToValue = value - floor.getKey();
            final double interpolationRatio = floorToValue / floorToCeiling;

            final double fPercentToCPercent = ceiling.getValue() - floor.getValue();
            final double scaled = fPercentToCPercent * interpolationRatio;
            final double shifted = scaled + floor.getValue();

            return shifted;
        }

        @Override public Iterator<Percentile> iterator() {
            return percents.iterator();
        }
    }
}
