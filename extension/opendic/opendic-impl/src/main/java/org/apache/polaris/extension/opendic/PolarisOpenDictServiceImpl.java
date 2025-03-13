package org.apache.polaris.extension.opendic;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.extension.opendic.api.PolarisObjectsApiService;
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.apache.polaris.extension.opendic.model.Udo;
import org.apache.polaris.extension.opendic.model.Udos;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Concrete implementation of the Polaris API services */
@RequestScoped
public class PolarisOpenDictServiceImpl implements PolarisObjectsApiService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolarisOpenDictServiceImpl.class);

    private final RealmEntityManagerFactory entityManagerFactory;
    private final PolarisAuthorizer polarisAuthorizer;
    private final MetaStoreManagerFactory metaStoreManagerFactory;
    private final CallContext callContext;

    @Inject
    public PolarisOpenDictServiceImpl(
        RealmEntityManagerFactory entityManagerFactory,
        MetaStoreManagerFactory metaStoreManagerFactory,
        PolarisAuthorizer polarisAuthorizer,
        CallContext callContext
    ) {
        this.entityManagerFactory = entityManagerFactory;
        this.metaStoreManagerFactory = metaStoreManagerFactory;
        this.polarisAuthorizer = polarisAuthorizer;
        this.callContext = callContext;
        // FIXME: This is a hack to set the current context for downstream calls.
        CallContext.setCurrentContext(callContext);
    }

    // Reuse the same helper method from PolarisServiceImpl
    private PolarisOpenDictService newAdminService(
        RealmContext realmContext,
        SecurityContext securityContext
    ) {
        AuthenticatedPolarisPrincipal authenticatedPrincipal =
            (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
        if (authenticatedPrincipal == null) {
            throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
        }
        // Create a new admin service with the right context
        PolarisEntityManager entityManager = entityManagerFactory.getOrCreateEntityManager(realmContext);
        PolarisMetaStoreManager metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(
            realmContext
        );
        return new PolarisOpenDictService(
            callContext,
            entityManager,
            metaStoreManager,
            securityContext,
            polarisAuthorizer
        );
    }

    @Override
    /**
     * List all objects of UDO: {type}
     */
    public Response listUdoObjects(String type, RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        // Implement your extension logic here

        LOGGER.info("list of {} returning:", type);
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }
    @Override
    public Response listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        // Implement your extension logic here

        LOGGER.info("list of all UDO types returning: {}");
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }

    @Override
    public Response createUdo(
        String type,
        CreateUdoRequest request,
        RealmContext realmContext,
        SecurityContext securityContext
    ) {
        PolarisAdminService adminService = newAdminService(realmContext, securityContext);
        // Implement your extension logic here
        LOGGER.info("Created new {} UDO {}", type, request.getObject().getName());
        return Response.status(Response.Status.NOT_IMPLEMENTED).build();
    }
}
