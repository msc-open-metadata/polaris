package org.apache.polaris.extension.opendic;

import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.apache.polaris.extension.opendic.model.Udo;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisOpenDictService extends PolarisAdminService {

    // Initialized in the authorize methods.
    private PolarisResolutionManifest resolutionManifest = null;

    public PolarisOpenDictService(
        @NotNull CallContext callContext,
        @NotNull PolarisEntityManager entityManager,
        @NotNull PolarisMetaStoreManager metaStoreManager,
        @NotNull SecurityContext securityContext,
        @NotNull PolarisAuthorizer authorizer
    ) {
        super(callContext, entityManager, metaStoreManager, securityContext, authorizer);
    }

    // TODO: Use PolarisEntity or OpenDictEntity
    // TODO: Create new OpenDictAuthorizableOperation.
    public Object createUdo(Udo obj) {
        PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG;
        super.authorizeBasicRootOperationOrThrow(op);
        return null;
    }

    // TODO: Use PolarisEntity or OpenDictEntity
    public List<Object> listUdoObjects(RealmContext realmContext, SecurityContext securityContext) {
        authorizeBasicRootOperationOrThrow(PolarisAuthorizableOperation.LIST_CATALOGS);
        return null;
    }
}
