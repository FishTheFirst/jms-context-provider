
package io.github.fishthefirst.contextproviders;

import io.github.fishthefirst.contextwrapper.JMSContextWrapper;

public interface JMSContextWrapperSupplier extends AutoCloseable {
    JMSContextWrapper createContext();
    JMSContextWrapper createContext(int sessionMode);
}
