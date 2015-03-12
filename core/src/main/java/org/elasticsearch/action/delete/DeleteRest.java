package org.elasticsearch.action.delete;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.AbstractRestClientAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.InputStreams.stripNulls;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireBoolean;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireLong;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.longToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.replicationTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.timeValueToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.versionTypeToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.writeConsistencyLevelToString;
import static com.bazaarvoice.elasticsearch.client.core.util.Validation.notNull;
import static org.elasticsearch.common.base.Optional.fromNullable;

public class DeleteRest extends AbstractRestClientAction<DeleteRequest, DeleteResponse> {

    public DeleteRest(final String protocol, final String host, final int port, final HttpExecutor executor) {
        super(protocol, host, port, executor);
    }

    @Override public ListenableFuture<DeleteResponse> act(final DeleteRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .protocol(protocol).host(host).port(port)
            .path(notNull(request.index())).seg(notNull(request.type())).seg(notNull(request.id()))
            .paramIfPresent("routing", fromNullable(request.routing()))
                // note parent(string) seems just to set the routing, so we don't need to provide it here
            .paramIfPresent("timeout", fromNullable(request.timeout()).transform(timeValueToString))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("version", fromNullable(request.version()).transform(longToString))
            .paramIfPresent("version_type", fromNullable(request.versionType()).transform(versionTypeToString))
            .paramIfPresent("replication", fromNullable(request.replicationType()).transform(replicationTypeToString))
            .paramIfPresent("consistency", fromNullable(request.consistencyLevel()).transform(writeConsistencyLevelToString));
        return Futures.transform(executor.delete(url.url()), deleteResponseFn);
    }

    @Override public FutureCallback<DeleteResponse> callback(final ActionListener<DeleteResponse> listener) {
        return new DeleteCallback(listener);
    }

    private static Function<HttpResponse, DeleteResponse> deleteResponseFn = new Function<HttpResponse, DeleteResponse>() {

        @Override public DeleteResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(stripNulls(httpResponse.response())).mapAndClose();
                if (map.containsKey("error")) {
                    // FIXME use the right exception
                    throw new RuntimeException("Some kind of error: " + map.toString());
                }
                return new DeleteResponse(
                    requireString(map.get("_index")),
                    requireString(map.get("_type")),
                    requireString(map.get("_id")),
                    requireLong(map.get("_version")),
                    requireBoolean(map.get("found")));
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }
    };

}
