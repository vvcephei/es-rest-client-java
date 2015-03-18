package org.elasticsearch.action.index;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.InputStreams.stripNulls;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireBoolean;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireLong;
import static com.bazaarvoice.elasticsearch.client.core.util.MapFunctions.requireString;

public class IndexResponseTransform implements Function<HttpResponse, IndexResponse> {
    @Override public IndexResponse apply(final HttpResponse httpResponse) {
        try {
            //TODO check REST status and "ok" field and handle failure
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(stripNulls(httpResponse.response())).mapAndClose();
            if (map.containsKey("error")) {
                // FIXME use the right exception
                throw new RuntimeException("Some kind of error: " + map.toString());
            }
            return new IndexResponse(
                requireString(map.get("_index")),
                requireString(map.get("_type")),
                requireString(map.get("_id")),
                requireLong(map.get("_version")),
                requireBoolean(map.get("created")));
        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible.
            throw new RuntimeException(e);
        }
    }
}
