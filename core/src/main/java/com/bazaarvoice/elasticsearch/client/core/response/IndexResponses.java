package com.bazaarvoice.elasticsearch.client.core.response;

import com.bazaarvoice.elasticsearch.client.core.HttpResponse;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.util.concurrent.FutureCallback;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireList;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireLong;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;

public class IndexResponses {
    private static Function<HttpResponse, IndexResponse> indexResponseFunction = new Function<HttpResponse, IndexResponse>() {
        @Override public IndexResponse apply(final HttpResponse httpResponse) {
            try {
                //TODO check REST status and "ok" field and handle failure
                Map<String, Object> map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
                IndexResponse indexResponse = new IndexResponse(
                    requireString(map.get("_index")),
                    requireString(map.get("_type")),
                    requireString(map.get("_id")),
                    requireLong(map.get("_version")));
                if (map.containsKey("matches")) {
                    List<String> matches = requireList(map.get("matches"), String.class);
                    indexResponse.setMatches(matches);
                }
                return indexResponse;
            } catch (IOException e) {
                // FIXME: which exception to use? It should match ES clients if possible.
                throw new RuntimeException(e);
            }
        }
    };

    public static FutureCallback<HttpResponse> indexResponseCallback(final ActionListener<IndexResponse> listener) {
        return new FutureCallback<HttpResponse>() {
            @Override public void onSuccess(final HttpResponse httpResponse) {
                listener.onResponse(indexResponseFunction.apply(httpResponse));
            }

            @Override public void onFailure(final Throwable throwable) {
                // TODO transform failure
                listener.onFailure(throwable);
            }
        };
    }
}
