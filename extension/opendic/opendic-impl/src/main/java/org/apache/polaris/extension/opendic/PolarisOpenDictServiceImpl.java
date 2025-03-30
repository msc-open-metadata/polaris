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
        IBaseRepository icebergRepository = new IcebergRepository();
        return new PolarisOpenDictService(callContext, entityManager, metaStoreManager, securityContext, polarisAuthorizer, icebergRepository
        );
    }

    /**
     * List all objects of UDO: {type}
     * Path: {@code GET /api/opendic/v1/objects/{type}}
     */
    @Override
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

        Map<String, String> schemaMap = adminService.listUdoTypes(realmContext, securityContext);

        LOGGER.info(OPENDIC_MARKER, "Listed Types: {}", schemaMap);
        return Response.status(Response.Status.OK)
                .entity(schemaMap)
                .type(MediaType.APPLICATION_JSON)
                .build();
    }

    /**
     * Delete all UDOs of {@code type} and drop the schema.
     *
     * @param type The name/type of the UDO to delete
     */
    @Override
    public Response deleteUdo(String type, RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        var deleted = adminService.deleteUdoOfType(type);

        if (deleted) {
            LOGGER.info(OPENDIC_MARKER, "Deleted type: {}", type);
            return Response.status(Response.Status.OK)
                    .entity(Map.of("Deleted all objects of type", type))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else {
            LOGGER.error(OPENDIC_MARKER, "Failed to delete type: {}", type);
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(Map.of("Failed to delete type", type))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }


    /**
     * Create a new UDO object of type {@code type}
     * Path: @code POST /api/opendic/v1/objects/{type}
     *
     * @param type    The type of the UDO to create
     * @param request The request containing the UDO object to create (type, name, jsonprops)
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
            LOGGER.error(OPENDIC_MARKER, "Failed to validate object {} against schema: {}. Error: {}", request.getUdo().getName(), type, e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("Failed to validate object against schema", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create UDO {}: {}", request.getUdo().getName(), e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("UDO already exists", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IOException e) {
            LOGGER.error(OPENDIC_MARKER, "I/O error creating UDO {}: {}", request.getUdo().getName(), e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("I/O error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    /**
     * Define a new UDO type
     * Path: {@code POST /api/opendic/v1/objects}
     *
     * @param request The request containing the UDO type and its schema properties as json
     */
    @Override
    public Response defineUdo(DefineUdoRequest request, RealmContext realmContext, SecurityContext securityContext) {
        PolarisOpenDictService adminService = newAdminService(realmContext, securityContext);
        Preconditions.checkNotNull(request.getUdoType());
        LOGGER.info(OPENDIC_MARKER, "Defining new UDO type: {}", request.getUdoType());

        UserDefinedEntitySchema schema;
        try {
            schema = UserDefinedEntitySchema.fromRequest(request);
            var createdSchemaString = adminService.defineSchema(schema);
            LOGGER.info(OPENDIC_MARKER, "Defined new udo type {}", request.getUdoType());

            return Response.status(Response.Status.CREATED)
                    .entity(Map.of(request.getUdoType(), createdSchemaString))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (IllegalArgumentException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create schema: {}", e.getMessage(), e);
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } catch (AlreadyExistsException e) {
            LOGGER.error(OPENDIC_MARKER, "Failed to create schema {}: {}", request.getUdoType(), e.getMessage(), e);
            return Response.status(Response.Status.CONFLICT)
                    .entity(Map.of("message", e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

    }
}
