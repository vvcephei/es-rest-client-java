package org.elasticsearch.action;

import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.util.Map;

/**
 * A function that transforms {@link RestResponse}s
 * whose response body is some kind of XContent into the desired type.
 * <p/>
 * You must supply the implementation of fromXContent by passing a
 * {@link org.elasticsearch.action.FromXContent} to the constructor.
 *
 * @param <R> The desired output format.
 */
public class XContentResponseTransform<R> implements Function<RestResponse, R> {
    private final FromXContent<R> unmarshaller;

    public XContentResponseTransform(FromXContent<R> unmarshaller) {
        this.unmarshaller = unmarshaller;
    }

    @Override public R apply(final RestResponse restResponse) {
        try {
            //TODO check REST status and "ok" field and handle failure
            final Map<String, Object> map;
            if (restResponse.contentTypeLowerCase().contains("application/smile")) {
                map = SmileXContent.smileXContent.createParser(restResponse.response()).mapAndClose();
            } else {
                // assume json?
                // not sure why we sometimes get a bunch of 0x0 chars in the response?
                map = JsonXContent.jsonXContent.createParser(InputStreams.stripNullChars(restResponse.response())).mapAndClose();
            }
            if (map.containsKey("error")) {
                // FIXME use the right exception. see https://github.com/bazaarvoice/es-client-java/issues/3
                throw new RuntimeException("Some kind of error: " + map.toString());
            }

            return unmarshaller.fromXContent(map);
        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible. see https://github.com/bazaarvoice/es-client-java/issues/3
            throw new RuntimeException(e);
        }
    }
}
