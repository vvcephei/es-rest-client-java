package org.elasticsearch.action;

import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.bazaarvoice.elasticsearch.client.core.util.InputStreams;
import org.elasticsearch.ElasticSearchExceptionHelper;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.smile.SmileXContent;

import java.io.IOException;
import java.io.InputStream;
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
            final String contentTypes = Joiner.on(",").join(restResponse.contentTypeLowerCase());
            if (contentTypes.contains("application/smile")) {
                map = SmileXContent.smileXContent.createParser(restResponse.response()).mapAndClose();
            } else if (contentTypes.contains("application/json")) {
                final InputStream is = InputStreams.stripNullChars(restResponse.response());
                map = JsonXContent.jsonXContent.createParser(is).mapAndClose();
            } else {
                throw new RuntimeException(String.format("Could not parse response. Content-Type:[%s] Body:[%s]", restResponse.contentTypeLowerCase(), InputStreams.toString(InputStreams.stripNullChars(restResponse.response()))));
            }

            // If there was an error throw the proper exception
            if (map.containsKey("error")) {
                throw ElasticSearchExceptionHelper.getProperException(map);
            }

            return unmarshaller.fromXContent(map);

        } catch (IOException e) {
            // FIXME: which exception to use? It should match ES clients if possible. see https://github.com/bazaarvoice/es-client-java/issues/3
            throw new RuntimeException(e);
        }
    }
}
