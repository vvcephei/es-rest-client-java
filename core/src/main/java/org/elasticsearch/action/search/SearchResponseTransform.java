package org.elasticsearch.action.search;

import com.bazaarvoice.elasticsearch.client.core.spi.HttpResponse;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SearchResponseTransform implements Function<HttpResponse, SearchResponse> {
    @Override public SearchResponse apply(final HttpResponse httpResponse) {
        try {
            //TODO check REST status and "ok" field and handle failure
            final Map<String, Object> map;
            if (httpResponse.contentTypeLowerCase().contains("application/smile")){
                map = SmileXContent.smileXContent.createParser(httpResponse.response()).mapAndClose();
            } else {
                // assume json?
                map = JsonXContent.jsonXContent.createParser(httpResponse.response()).mapAndClose();
            }
            if (map.containsKey("error")) {
                // FIXME use the right exception
                throw new RuntimeException("Some kind of error: " + map.toString());
            }

            return SearchResponseHelper.fromXContent(map);
        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible.
            throw new RuntimeException(e);
        }
    }

    private String toString(final InputStream response) {
        StringBuffer stringBuffer = new StringBuffer();
        int charac;
        try {
            while((charac = response.read()) != -1){
              stringBuffer.append((char)charac);
            }
            return stringBuffer.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
