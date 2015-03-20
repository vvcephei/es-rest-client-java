package org.elasticsearch.action.search.helpers;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InternalAggregationsHelper {
    public static InternalAggregations fromXContent(final Map<String, Object> map) {
        if (!map.containsKey("aggregations")) {
            return new InternalAggregations(ImmutableList.<InternalAggregation>of());
        }

        // FIXME TO_PR make the api return enough info to deserialize the Aggregations. see https://github.com/bazaarvoice/es-client-java/issues/5
        // toXContent needs to serialize the type of the aggregations. We /could/
        // perform read-time coersion to get around this (but it would be messy),
        // but the api doesn't even return enough information to reconstruct the internal aggregation objects.
        // for example, an average aggregation returns only the average, but the
        // java object needs the sum and count in the constructor.
        return new NotImplementedInternalAggregations();
    }

    public static class NotImplementedException extends RuntimeException {
        public NotImplementedException(final String message) {
            super(message);
        }
    }

    /**
     * A special Aggregations implementation that informs you that Aggregations is not implemented
     */
    private static class NotImplementedInternalAggregations extends InternalAggregations {

        private final NotImplementedException EXCEPTION = new NotImplementedException("Aggregations is not implementable given the current format of the json responses. The ES api needs to serialize the type of the aggregation as well as enough information to construct aggregation objects from the json. Both of these conditions are currently unmet.");

        public NotImplementedInternalAggregations() {
            super(null);
        }

        public NotImplementedInternalAggregations(final List<InternalAggregation> aggregations) {
            super(null);
        }

        @Override public List<Aggregation> asList() {
            throw EXCEPTION;
        }

        @Override public Map<String, Aggregation> asMap() {
            throw EXCEPTION;
        }

        @Override public <A extends Aggregation> A get(final String name) {
            throw EXCEPTION;
        }

        @Override public Map<String, Aggregation> getAsMap() {
            throw EXCEPTION;
        }


        @Override public Iterator<Aggregation> iterator() {
            throw EXCEPTION;
        }

        @Override public void readFrom(final StreamInput in) throws IOException {
            throw EXCEPTION;
        }

        @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            throw EXCEPTION;
        }

        @Override public XContentBuilder toXContentInternal(final XContentBuilder builder, final Params params) throws IOException {
            throw EXCEPTION;
        }

        @Override public void writeTo(final StreamOutput out) throws IOException {
            throw EXCEPTION;
        }
    }
}
