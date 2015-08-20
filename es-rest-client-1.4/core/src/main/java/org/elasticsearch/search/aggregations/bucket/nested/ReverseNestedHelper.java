package org.elasticsearch.search.aggregations.bucket.nested;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class ReverseNestedHelper {
    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final long docCount = nodeLongValue(map.get("doc_count"));
        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return !s.equals("doc_count");
            }
        });
        final InternalAggregations aggregations = InternalAggregationsHelper.fromXContentUnwrapped(subAggsMap, manifest);
        return new InternalReverseNested(name, docCount, aggregations);
    }
}
