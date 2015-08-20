package org.elasticsearch.action;

import java.util.Map;

/**
 * The inverse interface of {@link org.elasticsearch.common.xcontent.ToXContent}
 * @param <T>
 */
public interface FromXContent<T> {
    public T fromXContent(final Map<String, Object> map);
}
