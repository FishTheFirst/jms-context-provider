package io.github.fishthefirst;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.jackson.MessageWithMetadataJacksonMixInConfigurator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;

@ConditionalOnClass({ObjectMapper.class, MessageWithMetadata.class, MessageWithMetadataJacksonMixInConfigurator.class})
@AutoConfigurationPackage
@AutoConfiguration
public final class JacksonMixinAutoConfiguration {
    public JacksonMixinAutoConfiguration(ObjectMapper objectMapper) {
        MessageWithMetadataJacksonMixInConfigurator.configureMixIn(objectMapper);
    }
}
