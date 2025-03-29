package org.apache.polaris.extension.opendic;

import com.google.common.base.Preconditions;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.extension.opendic.api.PolarisObjectsApiService;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.model.CreateUdoRequest;
import org.apache.polaris.extension.opendic.model.DefineUdoRequest;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.extension.opendic.persistence.IcebergRepository;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of the Polaris API services
 */
@RequestScoped
public class PolarisOpenDictServiceImpl implements PolarisObjectsApiService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolarisOpenDictServiceImpl.class);
    private static final Marker OPENDIC_MARKER = MarkerFactory.getMarker("OPENDIC");

    private final RealmEntityManagerFactory entityManagerFactory;
    private final PolarisAuthorizer polarisAuthorizer;
    private final MetaStoreManagerFactory metaStoreManagerFactory;
    private final CallContext callContext;

    @Inject
    public PolarisOpenDictServiceImpl(RealmEntityManagerFactory entityManagerFactory,
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
    private PolarisOpenDictService newAdminService(RealmContext realmContext, SecurityContext securityContext
    ) {
        AuthenticatedPolarisPrincipal authenticatedPrincipal = (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
        if (authenticatedPrincipal == null) {
            throw new NotAuthorizedException("Failed to find authenticatedPrincipal in SecurityContext");
        }
        // Create a new admin service with the right context
        PolarisEntityManager entityManager = entityManagerFactory.getOrCreateEntityManager(realmContext);
        PolarisMetaStoreManager metaStoreManager = metaStoreManagerFactory.getOrCreateMetaStoreManager(realmContext
        );
        IBaseRepository icebergRepository = new IcebergRepository("polaris");
        return new PolarisOpenDictService(callContext, entityManager, metaStoreManager, securityContext, polarisAuthorizer, icebergRepository
        );
    }

    @Override
    /**
     * List all objects of UDO: {type}
     * Path: {@code GET /api/opendic/v1/objects/{type}}
     */
    public Response listUdoObjects(String type, RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);

        List<String> udosOfType = adminService.listUdosOfType(type);

        LOGGER.info(OPENDIC_MARKER, "Listing {}s: {}", type, udosOfType);
        return Response.status(Response.Status.OK)
                .entity(udosOfType)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * List all UDO types
     * Path: {@code GET /api/opendic/v1/objects}
     */
    @Override
    public Response listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);

        var schemaMap = adminService.listUdoTypes(realmContext, securityContext);

        LOGGER.info(OPENDIC_MARKER, "Listing Types: {}", schemaMap);
        return Response.status(Response.Status.OK)
                .entity(schemaMap)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }


    /**
     * Create a new UDO object of type {@code type}
     * Path: @code POST /api/opendic/v1/objects/{type}
     */
    @Override
    public Response createUdo(String type, CreateUdoRequest request, RealmContext realmContext, SecurityContext securityContext
    ) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(request.getUdo());
        LOGGER.info(OPENDIC_MARKER, "Creating open {} {} props: {}", type, request.getUdo().getName(), request.getUdo().getProps());
        try {
            UserDefinedEntity entity = UserDefinedEntity.fromRequest(type, request);
            var createdRecordString = adminService.createUdo(entity);
            return Response.status(Response.Status.CREATED)
                    .entity(Map.of(type + " " + request.getUdo().getName(), createdRecordString))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.info(OPENDIC_MARKER, "Failed to validate object {} against schema: {}. Error: {}, Trace: {}", request.getUdo().getName(), type, e.getMessage(), e.getStackTrace());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("Failed to validate object against schema", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.info(OPENDIC_MARKER, "Failed to create UDO {}: {}, Trace: {}", request.getUdo().getName(), e.getMessage(), e.getStackTrace());
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("UDO already exists", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            LOGGER.info(OPENDIC_MARKER, "I/O error creating UDO {}: {}, Trace: {}", request.getUdo().getName(), e.getMessage(), e.getStackTrace());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("I/O error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (RuntimeException e) {
            LOGGER.info(OPENDIC_MARKER, "Failed to create UDO {}: {}, Trace: {}", request.getUdo().getName(), e.getMessage(), e.getStackTrace());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("Failed to create UDO", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Define a new UDO type
     * Path: {@code POST /api/opendic/v1/objects}
     */
    @Override
    public Response defineUdo(DefineUdoRequest request, RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        UserDefinedEntitySchema schema;
        try {
            schema = UserDefinedEntitySchema.fromRequest(request);
            var createdSchemaString = adminService.defineSchema(schema);
            LOGGER.info(OPENDIC_MARKER, "Defined new udo type {}", request.getUdoType());
            LOGGER.info(OPENDIC_MARKER, "Schema: {}", createdSchemaString);

            return Response.status(Response.Status.CREATED)
                    .entity(Map.of(request.getUdoType(), createdSchemaString))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.info(OPENDIC_MARKER, "Failed to create schema: {}", e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.info(OPENDIC_MARKER, "Failed to create schema {}: {}", request.getUdoType(), e.getMessage());
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("error", "Schema already exists"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

    }
}
