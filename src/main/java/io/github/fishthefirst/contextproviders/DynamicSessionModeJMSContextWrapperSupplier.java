
package io.github.fishthefirst.contextproviders;

import io.github.fishthefirst.contextwrapper.JMSContextWrapper;

@FunctionalInterface
public interface DynamicSessionModeJMSContextWrapperSupplier {
    JMSContextWrapper createContext(int sessionMode);
}
