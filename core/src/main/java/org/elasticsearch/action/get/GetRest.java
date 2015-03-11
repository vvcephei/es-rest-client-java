package org.elasticsearch.action.get;

import com.bazaarvoice.elasticsearch.client.core.HttpExecutor;
import com.bazaarvoice.elasticsearch.client.core.HttpResponse;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.readBytesReference;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireMap;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.commaDelimitedToString;
import static com.bazaarvoice.elasticsearch.client.core.util.Validation.notNull;
import static org.elasticsearch.common.base.Optional.fromNullable;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeLongValue;

public class GetRest {
    private final String protocol;
    private final String host;
    private final int port;
    private final HttpExecutor executor;

    public GetRest(final String protocol, final String host, final int port, final HttpExecutor executor) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;

        this.executor = executor;
    }

    public ListenableFuture<GetResponse> act(GetRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .protocol(protocol).host(host).port(port)
            .path(notNull(request.index())).seg(notNull(request.type())).seg(notNull(request.id()))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("routing", fromNullable(request.routing()))
                // note parent(string) seems just to set the routing, so we don't need to provide it here
            .paramIfPresent("preference", fromNullable(request.preference()))
            .paramIfPresent("realtime", fromNullable(request.realtime()).transform(booleanToString))
            .paramIfPresent("ignore_errors_on_generated_fields", fromNullable(request.ignoreErrorsOnGeneratedFields()).transform(booleanToString))
            .paramIfPresent("fields", fromNullable(request.fields()).transform(commaDelimitedToString));

        return Futures.transform(executor.get(url.url()), getResponseFunction);
    }

    public static FutureCallback<GetResponse> callback(ActionListener<GetResponse> listener) {
        return new GetCallback(listener);
    }

    private static Function<HttpResponse, GetResponse> getResponseFunction = new Function<HttpResponse, GetResponse>() {
        @Override public GetResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(stripNulls(httpResponse.response())).mapAndClose();

                final Map<String, GetField> fields;
                if (map.containsKey("fields")) {
                    Map<String, Object> incoming = requireMap(map.get("fields"), String.class, Object.class);
                    fields = Maps.newHashMapWithExpectedSize(incoming.size());
                    for (Map.Entry<String, Object> entry : incoming.entrySet()) {
                        if (entry.getValue() instanceof List) {
                            fields.put(entry.getKey(), new GetField(entry.getKey(), requireList(entry.getValue(), Object.class)));
                        } else {
                            fields.put(entry.getKey(), new GetField(entry.getKey(), ImmutableList.of(entry.getValue())));
                        }
                    }
                } else {
                    fields = ImmutableMap.of();
                }

                return new GetResponse(new GetResult(
                    requireString(map.get("_index")),
                    requireString(map.get("_type")),
                    requireString(map.get("_id")),
                    nodeLongValue(map.get("_version"), -1),
                    nodeBooleanValue(map.get("found"), true),
                    readBytesReference(map.get("_source")),
                    fields
                ));
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }

        private void copy(final InputStream response, final StringWriter stringWriter) {
            int character;
            try {
                while ((character = response.read()) != -1) {
                    stringWriter.write(character);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    response.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private InputStream stripNulls(final InputStream inputStream) {
            // ES is adding null characters in the response stream. Not sure why.
            return new InputStream() {
                @Override public int read() throws IOException {
                    int read;
                    while ((read = inputStream.read()) == 0) {}
                    return read;
                }

                @Override public void close() throws IOException {
                    inputStream.close();
                }
            };
        }
    };

    private static class GetCallback implements FutureCallback<GetResponse> {
        private final ActionListener<GetResponse> listener;

        private GetCallback(final ActionListener<GetResponse> listener) {
            this.listener = listener;
        }

        @Override public void onSuccess(final GetResponse response) {
            listener.onResponse(response);
        }

        @Override public void onFailure(final Throwable throwable) {
            // TODO transform failure
            listener.onFailure(throwable);
        }
    }
}
