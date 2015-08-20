package org.elasticsearch.action;

import org.elasticsearch.common.util.concurrent.FutureCallback;

/**
 * Adapts the provided {@link org.elasticsearch.action.ActionListener} to a
 * {@link org.elasticsearch.common.util.concurrent.FutureCallback}
 *
 * @param <ResponseType>
 */
public class NotifyingCallback<ResponseType> implements FutureCallback<ResponseType> {
    private final ActionListener<ResponseType> listener;

    public static <ResponseType> FutureCallback<ResponseType> callback(ActionListener<ResponseType> listener) {
        return new NotifyingCallback<ResponseType>(listener);
    }

    private NotifyingCallback(final ActionListener<ResponseType> listener) {
        this.listener = listener;
    }

    @Override public void onSuccess(final ResponseType response) {
        listener.onResponse(response);
    }

    @Override public void onFailure(final Throwable throwable) {
        // TODO transform failure
        listener.onFailure(throwable);
    }
}