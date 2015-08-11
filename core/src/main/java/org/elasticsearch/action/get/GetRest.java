package org.elasticsearch.action.get;

import com.bazaarvoice.elasticsearch.client.core.spi.RestExecutor;
import com.bazaarvoice.elasticsearch.client.core.spi.RestResponse;
import com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder;
import org.elasticsearch.action.AbstractRestClientAction;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.util.concurrent.Futures;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.booleanToString;
import static com.bazaarvoice.elasticsearch.client.core.util.StringFunctions.commaDelimitedToString;
import static com.bazaarvoice.elasticsearch.client.core.util.UrlBuilder.urlEncode;
import static com.bazaarvoice.elasticsearch.client.core.util.Validation.notNull;
import static org.elasticsearch.common.base.Optional.fromNullable;

/**
 * The inverse of {@link org.elasticsearch.rest.action.get.RestGetAction}
 *
 * @param <ResponseType>
 */
public class GetRest<ResponseType> extends AbstractRestClientAction<GetRequest, ResponseType> {
    public GetRest(final String protocol, final String host, final int port, final RestExecutor executor, final Function<RestResponse, ResponseType> responseTransform) {
        super(protocol, host, port, executor, responseTransform);
    }

    @Override public ListenableFuture<ResponseType> act(GetRequest request) {
        UrlBuilder url = UrlBuilder.create()
            .protocol(protocol).host(host).port(port)
            .path(urlEncode(notNull(request.index())))
            .seg(urlEncode(notNull(request.type())))
            .seg(urlEncode(notNull(request.id())))
            .paramIfPresent("refresh", fromNullable(request.refresh()).transform(booleanToString))
            .paramIfPresent("routing", fromNullable(request.routing()))
                // note parent(string) seems just to set the routing, so we don't need to provide it here
            .paramIfPresent("preference", fromNullable(request.preference()))
            .paramIfPresent("realtime", fromNullable(request.realtime()).transform(booleanToString))
            .paramIfPresent("ignore_errors_on_generated_fields", fromNullable(request.ignoreErrorsOnGeneratedFields()).transform(booleanToString))
            .paramIfPresent("fields", fromNullable(request.fields()).transform(commaDelimitedToString));

        return Futures.transform(executor.get(url.url()), responseTransform);
    }
}
