package org.apache.polaris.extension.opendic;

import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.opendic.entity.DeprecatedPolarisUserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.service.admin.PolarisAdminService;

import java.util.List;
import java.util.Map;

public class PolarisOpenDictService extends PolarisAdminService {

    // Initialized in the authorize methods.
    private final PolarisResolutionManifest resolutionManifest = null;
    private final IBaseRepository icebergRepository;

    public PolarisOpenDictService(
            @NotNull CallContext callContext,
            @NotNull PolarisEntityManager entityManager,
            @NotNull PolarisMetaStoreManager metaStoreManager,
            @NotNull SecurityContext securityContext,
            @NotNull PolarisAuthorizer authorizer,
            @NotNull IBaseRepository icebergRepository
    ) {
        super(callContext, entityManager, metaStoreManager, securityContext, authorizer);
        this.icebergRepository = icebergRepository;
    }

    // TODO: Create new OpenDictAuthorizableOperation?
    public DeprecatedPolarisUserDefinedEntity createUdo(PolarisEntity entity) {
        PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG;
        super.authorizeBasicRootOperationOrThrow(op);

        //TODO Hardcoded. Register default in schema registry and get schemas from there.
        UserDefinedEntitySchema udoSchema = new UserDefinedEntitySchema.Builder("FUNCTION")
                .setProperties(UserDefinedEntitySchema.propsFromMap(Map.of("def", "string", "language", "string", "version", "int", "params", "map")))
                .build();

        PolarisEntity returnedEntity =
                PolarisEntity.of(
                        metaStoreManager.createEntityIfNotExists(
                                getCurrentPolarisContext(),
                                null,
                                new PolarisEntity.Builder(entity)
                                        .setId(metaStoreManager.generateNewEntityId(getCurrentPolarisContext()).getId())
                                        .setCreateTimestamp(System.currentTimeMillis())
                                        .build()));
        if (returnedEntity == null) {
            throw new AlreadyExistsException(
                    "Cannot create Entity %s. Entity already exists or resolution failed",
                    entity.getName());
        }
        return null;
    }


    // TODO: Use PolarisEntity or OpenDictEntity
    public List<DeprecatedPolarisUserDefinedEntity> listUdoObjects(RealmContext realmContext, SecurityContext securityContext) {
        authorizeBasicRootOperationOrThrow(PolarisAuthorizableOperation.LIST_CATALOGS);
        return null;
    }

    public List<String> defineSchema(UserDefinedEntitySchema schema) {
//        //TODO hardcoded
//        UserDefinedEntitySchema schema = new UserDefinedEntitySchema.Builder("FUNCTION")
//                .setProperties(UserDefinedEntitySchema.propsFromMap(Map.of("def", "string", "language", "string", "version", "int", "params", "map")))
//                .build();

        var created_schema = icebergRepository.createTable("SYSTEM", schema.typeName(), UserDefinedEntitySchema.getIcebergSchema(schema));
        if (created_schema == null) {
            throw new AlreadyExistsException(
                    "Cannot create UDO %s. Entity already exists or resolution failed",
                    schema.typeName());
        }
        return created_schema;
    }
}
