package org.elasticsearch.search.aggregations.metrics.geobounds;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;

public class GeoBoundsHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final Map<String, Object> bounds = nodeMapValue(map.get("bounds"), String.class, Object.class);
        final Map<String, Number> topLeft = nodeMapValue(bounds.get("top_left"), String.class, Number.class);
        final Map<String, Number> bottomRight = nodeMapValue(bounds.get("bottom_right"), String.class, Number.class);
        return new ComputedGeoBounds(
            name,
            new GeoPoint(topLeft.get("lat").doubleValue(), topLeft.get("lon").doubleValue()),
            new GeoPoint(bottomRight.get("lat").doubleValue(), bottomRight.get("lon").doubleValue()));
    }

    private final static class ComputedGeoBounds extends AbstractInternalAggregation implements GeoBounds {

        private final GeoPoint topLeft;
        private final GeoPoint bottomRight;

        private ComputedGeoBounds(final String name, final GeoPoint topLeft, final GeoPoint bottomRight) {
            super(name);
            this.topLeft = topLeft;
            this.bottomRight = bottomRight;
        }

        @Override public GeoPoint topLeft() {
            return topLeft;
        }

        @Override public GeoPoint bottomRight() {
            return bottomRight;
        }
    }
}
