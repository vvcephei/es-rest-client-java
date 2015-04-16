package org.elasticsearch.action.search.helpers;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTermsHelper;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTermsHelper;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTermsHelper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.nodeMapValue;

public class InternalAggregationsHelper {
    public static InternalAggregations fromXContent(final Map<String, Object> map) {
        if (!map.containsKey("aggregations")) {
            return new InternalAggregations(ImmutableList.<InternalAggregation>of());
        }

        final ImmutableList.Builder<InternalAggregation> builder = ImmutableList.builder();
        final Map<String, Object> aggregationsMap = nodeMapValue(map.get("aggregations"), String.class, Object.class);
        for (Map.Entry<String, Object> aggregation : aggregationsMap.entrySet()) {
            final Map<String, Object> aggregationMap = nodeMapValue(aggregation.getValue(), String.class, Object.class);
            builder.add(InternalAggregationHelper.fromXContent(aggregation.getKey(), aggregationMap));
        }

        return new InternalAggregations(builder.build());
    }

    public static class UnrealizedAggregations extends InternalAggregations {
        private final Map<String, Object> map;

        /**
         * Constructs a new addAggregation.
         *
         * @param aggregationsMap
         */
        public UnrealizedAggregations(final Map<String, Object> aggregationsMap) {
            super(null); // we're completely shadowing InternalAggregations
            this.map = aggregationsMap;
        }

        public DoubleTerms getDoubleTerms(final String name) {
            if (map.containsKey(name)) {
                final Map<String, Object> doubleTermsMap = nodeMapValue(map.get(name), String.class, Object.class);
                return DoubleTermsHelper.fromXContent(name, doubleTermsMap);
            } else {
                return null;
            }
        }


        public LongTerms getLongTerms(final String name) {
            if (map.containsKey(name)) {
                final Map<String, Object> longTermsMap = nodeMapValue(map.get(name), String.class, Object.class);
                return LongTermsHelper.fromXContent(name, longTermsMap);
            } else {
                return null;
            }
        }


        public StringTerms getStringTerms(final String name) {
            if (map.containsKey(name)) {
                final Map<String, Object> longTermsMap = nodeMapValue(map.get(name), String.class, Object.class);
                return StringTermsHelper.fromXContent(name, longTermsMap);
            } else {
                return null;
            }
        }

        @Override public Iterator<Aggregation> iterator() {
            throw new NotImplementedException();
        }

        @Override public List<Aggregation> asList() {
            throw new NotImplementedException();
        }

        @Override public Map<String, Aggregation> asMap() {
            throw new NotImplementedException();
        }

        @Override public Map<String, Aggregation> getAsMap() {
            throw new NotImplementedException();
        }

        @Override public <A extends Aggregation> A get(final String name) {
            throw new NotImplementedException();
        }

        @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            throw new NotImplementedException();
        }

        @Override public XContentBuilder toXContentInternal(final XContentBuilder builder, final Params params) throws IOException {
            throw new NotImplementedException();
        }

        @Override public void readFrom(final StreamInput in) throws IOException {
            throw new NotImplementedException();
        }

        @Override public void writeTo(final StreamOutput out) throws IOException {
            throw new NotImplementedException();
        }

    }

    public static class NotImplementedException extends RuntimeException {
        public NotImplementedException() {
        }

        public NotImplementedException(final Throwable cause) {
            super(cause);
        }

        public NotImplementedException(final String message) {
            super(message);
        }

        public NotImplementedException(final String message, final Throwable cause) {
            super(message, cause);
        }

        public NotImplementedException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
