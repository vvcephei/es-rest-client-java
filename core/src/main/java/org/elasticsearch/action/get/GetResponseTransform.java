package org.elasticsearch.action.get;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

import static com.bazaarvoice.elasticsearch.client.core.util.InputStreams.stripNulls;

public class GetResponseTransform implements Function<HttpResponse, GetResponse> {
    @Override public GetResponse apply(final HttpResponse httpResponse) {
        try {
            //TODO check REST status and "ok" field and handle failure
            Map<String, Object> map = JsonXContent.jsonXContent.createParser(stripNulls(httpResponse.response())).mapAndClose();
            if (map.containsKey("error")) {
                // FIXME use the right exception
                throw new RuntimeException("Some kind of error: " + map.toString());
            }
            return GetHelper.fromXContent(map);
        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible.
            throw new RuntimeException(e);
        }
    }
}
