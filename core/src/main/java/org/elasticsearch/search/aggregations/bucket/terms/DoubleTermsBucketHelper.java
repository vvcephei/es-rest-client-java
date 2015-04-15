package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.search.helpers.InternalAggregationHelper;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.util.Map;
import java.util.Set;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class DoubleTermsBucketHelper {
    private static final String KEY = "key";
    private static final String DOC_COUNT = "doc_count";
    private static final String DOC_COUNT_ERROR_UPPER_BOUND = "doc_count_error_upper_bound";
    private static final Set<String> properKeys = ImmutableSet.of(KEY, DOC_COUNT, DOC_COUNT_ERROR_UPPER_BOUND);

    public static DoubleTerms.Bucket fromXContent(final Map<String, Object> map) {
        final double term = nodeDoubleValue(map.get(KEY));
        // optional field "key_as_string"
        final long doc_count = nodeLongValue(map.get(DOC_COUNT));
        final boolean show_doc_count_error = map.containsKey(DOC_COUNT_ERROR_UPPER_BOUND);
        final Long doc_count_error_upper_bound = nodeLongValue(DOC_COUNT_ERROR_UPPER_BOUND, -1);
        final Sets.SetView<String> subAggregationKeys = Sets.difference(map.keySet(), properKeys);
        final ImmutableList.Builder<InternalAggregation> subAggregations = ImmutableList.builder();
        for (String subAggregationKey : subAggregationKeys) {
            final Map<String, Object> subAggregation = nodeMapValue(map.get(subAggregationKey), String.class, Object.class);
            subAggregations.add(InternalAggregationHelper.fromXContent(subAggregationKey, subAggregation));
        }
        final InternalAggregations aggregations = new InternalAggregations(subAggregations.build());
        return new DoubleTerms.Bucket(term, doc_count, aggregations, show_doc_count_error, doc_count_error_upper_bound);
    }
}
