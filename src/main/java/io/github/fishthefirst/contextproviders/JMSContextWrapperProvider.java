
package io.github.fishthefirst.contextproviders;

import io.github.fishthefirst.contextwrapper.JMSContextWrapper;

public interface JMSContextWrapperProvider extends AutoCloseable {
    JMSContextWrapper createContext();
    JMSContextWrapper createContext(int sessionMode);
}
