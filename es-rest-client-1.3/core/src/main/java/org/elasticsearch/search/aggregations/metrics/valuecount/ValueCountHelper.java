package org.elasticsearch.search.aggregations.metrics.valuecount;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class ValueCountHelper {
    public static InternalValueCount fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final long value = nodeLongValue(map.get("value")); ;
        return new InternalValueCount(name, value);
    }
}
