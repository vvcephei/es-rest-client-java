package org.elasticsearch.search.aggregations.bucket.significant;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.helpers.InternalAggregationsHelper;
import org.elasticsearch.common.base.Predicate;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.search.aggregations.Aggregations;

import java.util.Map;
import java.util.Set;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeStringValue;
import static org.elasticsearch.common.Preconditions.checkState;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeDoubleValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class SignificantTermsBucketHelper {

    public static final String KEY = "key";
    public static final String DOC_COUNT = "doc_count";
    public static final String BG_COUNT = "bg_count";
    public static final String SCORE = "score";
    private static final Set<String> properKeys = ImmutableSet.of(KEY, DOC_COUNT, BG_COUNT, SCORE);

    public static SignificantTerms.Bucket fromXContent(final long aggDocCount, final Map<String, Object> map, final AggregationsManifest manifest) {
        final Object untypedTerm = map.get(KEY);
        final long bucketDocCount = nodeLongValue(map.get(DOC_COUNT));
        final long bucketBgCount = nodeLongValue(map.get(BG_COUNT));
        final double score = nodeDoubleValue(map.get(SCORE));
        final Map<String, Object> subAggsMap = Maps.filterKeys(map, new Predicate<String>() {
            @Override public boolean apply(final String s) {
                return !properKeys.contains(s);
            }
        });
        final Aggregations aggregations = InternalAggregationsHelper.fromXContentUnwrapped(subAggsMap, manifest);
        if (untypedTerm instanceof String) {
            return new ComputedSignificantStringTermsBucket(new BytesRef(nodeStringValue(untypedTerm)), bucketDocCount, aggDocCount, bucketBgCount, score, aggregations);
        } else {
            checkState(untypedTerm instanceof Number);
            //noinspection ConstantConditions
            return new ComputedSignificantNumberTermsBucket((Number) untypedTerm, bucketDocCount, aggDocCount, bucketBgCount, score, aggregations);
        }
    }

    private static abstract class ComputedSignificantTermsBucket extends SignificantTerms.Bucket {

        private ComputedSignificantTermsBucket(final long bucketDocCount, final long aggDocCount, final long bucketBgCount) {
            super(bucketDocCount, aggDocCount, bucketBgCount, -1/*FIXME unserialized*/);
        }

        @Override public long getSupersetSize() {
            // FIXME unserialized
            throw new UnsupportedOperationException("unserialized information");
        }


        @Override public long getDocCount() {
            return getSubsetDf();
        }
    }

    private static class ComputedSignificantStringTermsBucket extends ComputedSignificantTermsBucket {

        private BytesRef termBytes;
        private double score;
        private Aggregations aggregations;

        private ComputedSignificantStringTermsBucket(final BytesRef term, final long bucketDocCount, final long aggDocCount, final long bucketBgCount, final double score, final Aggregations aggregations) {
            super(bucketDocCount, aggDocCount, bucketBgCount);
            this.aggregations = aggregations;
            this.score = score;
            this.termBytes = term;
        }

        @Override public Number getKeyAsNumber() {
            return Double.parseDouble(termBytes.utf8ToString());
        }

        @Override public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override int compareTerm(final SignificantTerms.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((ComputedSignificantStringTermsBucket) other).termBytes);
        }

        @Override public double getSignificanceScore() {
            return score;
        }

        @Override public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
        }


        @Override public Aggregations getAggregations() {
            return aggregations;
        }
    }

    private static class ComputedSignificantNumberTermsBucket extends ComputedSignificantTermsBucket {

        private Number term;
        private double score;
        private Aggregations aggregations;

        private ComputedSignificantNumberTermsBucket(final Number term, final long bucketDocCount, final long aggDocCount, final long bucketBgCount, final double score, final Aggregations aggregations) {
            super(bucketDocCount, aggDocCount, bucketBgCount);
            this.aggregations = aggregations;
            this.score = score;
            this.term = term;
        }

        @Override public Number getKeyAsNumber() {
            return term;
        }

        @Override int compareTerm(final SignificantTerms.Bucket other) {
            return Long.compare(term.longValue(), other.getKeyAsNumber().longValue());
        }

        @Override public double getSignificanceScore() {
            return score;
        }

        @Override public String getKey() {
            return term.toString();
        }

        @Override public Text getKeyAsText() {
            return new StringText(getKey());
        }

        @Override public Aggregations getAggregations() {
            return aggregations;
        }
    }
}
