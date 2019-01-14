/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import org.junit.jupiter.api.Tag;

@CCMConfig(numberOfNodes = 3)
@Tag("long")
class M3PTokenPartitionerCCMIT extends PartitionerCCMITBase {

  M3PTokenPartitionerCCMIT(CCMCluster ccm, @SessionConfig(useKeyspace = NONE) Session session) {
    super(ccm, session, false);
  }
}
