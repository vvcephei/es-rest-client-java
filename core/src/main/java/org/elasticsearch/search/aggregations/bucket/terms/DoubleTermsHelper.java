package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class DoubleTermsHelper {
    public static final String DOC_COUNT_ERROR_UPPER_BOUND = "doc_count_error_upper_bound";
    public static final String SUM_OTHER_DOC_COUNT = "sum_other_doc_count";
    public static final String BUCKETS = "buckets";

    public static DoubleTerms fromXContent(final String name, final Map<String, Object> map) {
        final long doc_count_error_upper_bound = nodeLongValue(map.get(DOC_COUNT_ERROR_UPPER_BOUND));
        final long sum_other_doc_count = nodeLongValue(map.get(SUM_OTHER_DOC_COUNT));
        final List<Object> bucketObjs = nodeListValue(map.get(BUCKETS), Object.class);
        final ImmutableList.Builder<InternalTerms.Bucket> buckets = ImmutableList.builder();
        for (Object bucketObj : bucketObjs) {
            final Map<String, Object> bucket = nodeMapValue(bucketObj, String.class, Object.class);
            buckets.add(DoubleTermsBucketHelper.fromXContent(bucket));
        }
        final InternalOrder countDesc = InternalOrder.COUNT_DESC; // Note we could try to infer this, supposing the map is an ordered map of some kind.
        final ValueFormatter valueFormatter = ValueFormatter.RAW; // This looks like the right choice...
        final int requiredSize = -1; // This seems to be unnecessary for our use case...
        final int shardSize = -1; // This seems to be unnecessary for our use case...
        final int minDocCount = -1; // This seems to be unnecessary for our use case...
        return new DoubleTerms(name, countDesc, valueFormatter,requiredSize, shardSize, minDocCount, buckets.build(), false, -1,sum_other_doc_count);
    }
}
