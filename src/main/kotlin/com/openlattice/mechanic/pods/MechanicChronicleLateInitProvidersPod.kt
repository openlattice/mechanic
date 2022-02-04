package com.openlattice.mechanic.pods

import com.openlattice.chronicle.providers.LateInitProvider
import com.openlattice.chronicle.providers.OnUseLateInitProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class MechanicChronicleLateInitProvidersPod {
    @Bean
    fun chronicleLateInitProvider() : LateInitProvider = OnUseLateInitProvider()
}