package org.elasticsearch.search.aggregations.bucket.terms;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;
import static org.elasticsearch.common.base.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class TermsHelper {
    public static final String DOC_COUNT_ERROR_UPPER_BOUND = "doc_count_error_upper_bound";
    public static final String SUM_OTHER_DOC_COUNT = "sum_other_doc_count";
    public static final String BUCKETS = "buckets";

    public static InternalTerms fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest subAggregationsManifest) {
        final long doc_count_error_upper_bound = nodeLongValue(map.get(DOC_COUNT_ERROR_UPPER_BOUND));
        final long sum_other_doc_count = nodeLongValue(map.get(SUM_OTHER_DOC_COUNT));
        final List<Object> bucketObjs = nodeListValue(map.get(BUCKETS), Object.class);
        final ImmutableList.Builder<InternalTerms.Bucket> buckets = ImmutableList.builder();
        boolean isString = false;
        boolean isDouble = false;
        boolean isLong = false;
        for (Object bucketObj : bucketObjs) {
            final Map<String, Object> bucketMap = nodeMapValue(bucketObj, String.class, Object.class);
            final InternalTerms.Bucket bucket = TermsBucketHelper.fromXContent(bucketMap, subAggregationsManifest);
            if (bucket instanceof StringTerms.Bucket) {
                isString = true;
            } else if (bucket instanceof LongTerms.Bucket) {
                isLong = true;
            } else {
                isDouble = true;
            }
            buckets.add(bucket);
        }
        final InternalOrder countDesc = InternalOrder.COUNT_DESC; // Note we could try to infer this, supposing the map is an ordered map of some kind.
        final ValueFormatter valueFormatter = ValueFormatter.RAW; // This looks like the right choice...
        final int requiredSize = -1; // This seems to be unnecessary for our use case...
        final int shardSize = -1; // This seems to be unnecessary for our use case...
        final int minDocCount = -1; // This seems to be unnecessary for our use case...
        checkState(bucketObjs.isEmpty() || exactlyOneTrue(isString, isLong, isDouble));
        if (isString) {
            return new StringTerms(name, countDesc, requiredSize, shardSize, minDocCount, buckets.build(), false, -1, sum_other_doc_count);
        } else if (isLong) {
            return new LongTerms(name, countDesc, valueFormatter, requiredSize, shardSize, minDocCount, buckets.build(), false, -1, sum_other_doc_count);
        } else if (isDouble){
            return new DoubleTerms(name, countDesc, valueFormatter, requiredSize, shardSize, minDocCount, buckets.build(), false, -1, sum_other_doc_count);
        } else {
            checkState(bucketObjs.isEmpty());
            // it shouldn't matter
            return new StringTerms(name, countDesc, requiredSize, shardSize, minDocCount, buckets.build(), false, -1, sum_other_doc_count);
        }
    }

    private static boolean exactlyOneTrue(final Boolean... facts) {
        boolean onlyOne = false;
        for (boolean fact : facts) {
            if (fact && onlyOne) {
                return false;
            } else if (fact) {
                onlyOne = true;
            }
        }
        return onlyOne;
    }


}
