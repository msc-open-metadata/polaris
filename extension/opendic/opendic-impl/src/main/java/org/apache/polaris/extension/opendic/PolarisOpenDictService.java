package org.apache.polaris.extension.opendic;

import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.List;

public class PolarisOpenDictService extends PolarisAdminService {

    // Initialized in the authorize methods.
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PolarisOpenDictService.class);
    private static final Marker OPENDIC_MARKER = MarkerFactory.getMarker("OPENDIC");
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


    public String createUdo(UserDefinedEntity entity) throws IOException, AlreadyExistsException {
        PolarisAuthorizableOperation op = PolarisAuthorizableOperation.CREATE_CATALOG; //FIXME placeholder
        super.authorizeBasicRootOperationOrThrow(op);

        String namespace = "SYSTEM";
        var schema = icebergRepository.readTableSchema(namespace, entity.typeName());
        var genericRecord = icebergRepository.createGenericRecord(schema, entity.props());
        icebergRepository.insertRecord(namespace, entity.typeName(), genericRecord);
        return genericRecord.toString();

    }

    public List<String> listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_CATALOGS; //FIXME placeholder
        super.authorizeBasicRootOperationOrThrow(op);

        return icebergRepository.listEntityTypes("SYSTEM");
    }

    public List<String> listUdosOfType(String typeName) {
        PolarisAuthorizableOperation op = PolarisAuthorizableOperation.LIST_CATALOGS; //FIXME placeholder
        super.authorizeBasicRootOperationOrThrow(op);

        return icebergRepository.readRecords("SYSTEM", typeName)
                .stream()
                .map(GenericRecord::toString)
                .toList();
    }

    public String defineSchema(UserDefinedEntitySchema schema) throws AlreadyExistsException {
        return icebergRepository.createTable("SYSTEM", schema.typeName(), UserDefinedEntitySchema.getIcebergSchema(schema));

    }
}
