package org.elasticsearch.action.search.helpers;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public abstract class AbstractInternalAggregation extends InternalAggregation {
    public AbstractInternalAggregation(final String name) {
        super(name);
    }

    @Override public Type type() {
        throw new RuntimeException("not implemented");
    }

    @Override public InternalAggregation reduce(final ReduceContext reduceContext) {
        throw new RuntimeException("not implemented");
    }

    @Override public void readFrom(final StreamInput in) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override public void writeTo(final StreamOutput out) throws IOException {
        throw new RuntimeException("not implemented");
    }


    @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        throw new NotImplementedException();
    }
}
