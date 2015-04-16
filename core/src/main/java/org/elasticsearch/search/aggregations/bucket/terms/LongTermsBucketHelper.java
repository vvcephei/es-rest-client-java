package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class LongTermsBucketHelper {
    private static final String KEY = "key";
    private static final String DOC_COUNT = "doc_count";
    private static final String DOC_COUNT_ERROR_UPPER_BOUND = "doc_count_error_upper_bound";
    private static final Set<String> properKeys = ImmutableSet.of(KEY, DOC_COUNT, DOC_COUNT_ERROR_UPPER_BOUND);

    public static LongTerms.Bucket fromXContent(final Map<String, Object> map) {
        final long term = nodeLongValue(map.get(KEY));
        // optional field "key_as_string"
        final long doc_count = nodeLongValue(map.get(DOC_COUNT));
        final boolean show_doc_count_error = map.containsKey(DOC_COUNT_ERROR_UPPER_BOUND);
        final Long doc_count_error_upper_bound = nodeLongValue(DOC_COUNT_ERROR_UPPER_BOUND, -1);

        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return properKeys.contains(s);
            }
        });

        final InternalAggregationsHelper.UnrealizedAggregations subAggregations = new InternalAggregationsHelper.UnrealizedAggregations(subAggsMap);

        return new LongTerms.Bucket(term, doc_count, subAggregations, show_doc_count_error, doc_count_error_upper_bound);
    }
}
