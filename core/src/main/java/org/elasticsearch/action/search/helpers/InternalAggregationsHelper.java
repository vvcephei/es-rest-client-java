package org.elasticsearch.action.search.helpers;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
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
        } else {
            final Map<String, Object> aggregationsMap = nodeMapValue(map.get("aggregations"), String.class, Object.class);
            return new UnrealizedAggregations(aggregationsMap);
        }
    }

    public static class UnrealizedAggregations extends InternalAggregations {
        private final Map<String, Object> map;
        private volatile InternalAggregations realizedAggregations = null;

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
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.iterator();
        }

        @Override public List<Aggregation> asList() {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.asList();
        }

        @Override public Map<String, Aggregation> asMap() {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.asMap();
        }

        @Override public Map<String, Aggregation> getAsMap() {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.getAsMap();
        }

        @Override public <A extends Aggregation> A get(final String name) {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.get(name);
        }

        @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.toXContent(builder, params);
        }

        @Override public XContentBuilder toXContentInternal(final XContentBuilder builder, final Params params) throws IOException {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            return realizedAggregations.toXContentInternal(builder, params);
        }

        @Override public void readFrom(final StreamInput in) throws IOException {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            realizedAggregations.readFrom(in);
        }

        @Override public void writeTo(final StreamOutput out) throws IOException {
            if (realizedAggregations == null) {
                realizedAggregations = InternalAggregationsHelper.duckTypeParse(map);
            }
            realizedAggregations.writeTo(out);
        }

    }

    private static InternalAggregations duckTypeParse(final Map<String, Object> map) {
        // TODO try and parse
        throw new NotImplementedException();
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
