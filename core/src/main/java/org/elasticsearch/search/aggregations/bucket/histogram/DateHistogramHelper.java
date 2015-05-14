package org.elasticsearch.search.aggregations.bucket.histogram;

import com.bazaarvoice.elasticsearch.client.core.util.aggs.AggregationsManifest;
import org.elasticsearch.action.search.helpers.AbstractInternalAggregation;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeListValue;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;

public class DateHistogramHelper {

    public static InternalAggregation fromXContent(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final Object bucketsObj = map.get("buckets");
        if (bucketsObj instanceof Map) {
            final Map<String, Object> bucketMap = nodeMapValue(bucketsObj, String.class, Object.class);
            return internalKeyedHistogram(name, bucketMap, manifest);
        } else {
            final List<Object> bucketList = nodeListValue(bucketsObj, Object.class);
            return internalAnonHistogram(name, bucketList, manifest);
        }
    }

    private static InternalAggregation internalAnonHistogram(final String name, final List<Object> list, final AggregationsManifest manifest) {
        final ImmutableList.Builder<DateHistogram.Bucket> buckets = ImmutableList.builder();
        for (Object bucketObj : list) {
            final Map<String, Object> bucketMap = nodeMapValue(bucketObj, String.class, Object.class);
            buckets.add(DateHistogramBucketHelper.fromXContent(null, bucketMap, manifest));
        }
        return new ComputedDateHistogram(name, buckets.build());
    }

    private static InternalAggregation internalKeyedHistogram(final String name, final Map<String, Object> map, final AggregationsManifest manifest) {
        final ImmutableList.Builder<DateHistogram.Bucket> buckets = ImmutableList.builder();
        for (Map.Entry<String, Object> bucketObj : map.entrySet()) {
            final String bucketName = bucketObj.getKey();
            final Map<String, Object> bucketMap = nodeMapValue(bucketObj.getValue(), String.class, Object.class);
            buckets.add(DateHistogramBucketHelper.fromXContent(bucketName, bucketMap, manifest));
        }
        return new ComputedDateHistogram(name, buckets.build());
    }

    private static class ComputedDateHistogram extends AbstractInternalAggregation implements DateHistogram {

        private final ImmutableList<Bucket> buckets;
        private final ImmutableMap<String, Bucket> bucketStringMap;
        private final ImmutableMap<Number, Bucket> bucketNumberMap;
        private final ImmutableMap<DateTime, Bucket> bucketDateMap;

        public ComputedDateHistogram(final String name, final ImmutableList<Bucket> buckets) {
            super(name);
            this.buckets = buckets;
            this.bucketStringMap = buildStringMap(buckets);
            this.bucketDateMap = buildDateMap(buckets);
            this.bucketNumberMap = buildNumberMap(buckets);
        }

        private static ImmutableMap<String, Bucket> buildStringMap(final List<Bucket> buckets) {
            ImmutableMap.Builder<String, Bucket> bucketMap = ImmutableMap.builder();
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKey(), bucket);
            }
            return bucketMap.build();
        }

        private static ImmutableMap<Number, Bucket> buildNumberMap(final List<Bucket> buckets) {
            ImmutableMap.Builder<Number, Bucket> bucketMap = ImmutableMap.builder();
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsNumber(), bucket);
            }
            return bucketMap.build();
        }

        private static ImmutableMap<DateTime, Bucket> buildDateMap(final List<Bucket> buckets) {
            ImmutableMap.Builder<DateTime, Bucket> bucketMap = ImmutableMap.builder();
            for (Bucket bucket : buckets) {
                bucketMap.put(bucket.getKeyAsDate(), bucket);
            }
            return bucketMap.build();
        }

        @Override public List<? extends Bucket> getBuckets() {
            return buckets;
        }

        @Override public Bucket getBucketByKey(final String key) {

            return bucketStringMap.get(key);
        }

        @Override public Bucket getBucketByKey(final Number key) {
            return bucketNumberMap.get(key);
        }

        @Override public Bucket getBucketByKey(final DateTime dateTime) {
            return bucketDateMap.get(dateTime);
        }
    }
}
