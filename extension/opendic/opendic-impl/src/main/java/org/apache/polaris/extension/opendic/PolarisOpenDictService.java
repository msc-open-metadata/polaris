package org.apache.polaris.extension.opendic;

import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntity;
import org.apache.polaris.extension.opendic.entity.UserDefinedEntitySchema;
import org.apache.polaris.extension.opendic.entity.UserDefinedPlatformMapping;
import org.apache.polaris.extension.opendic.persistence.IBaseRepository;
import org.apache.polaris.service.admin.PolarisAdminService;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class PolarisOpenDictService extends PolarisAdminService {

    // Initialized in the authorize methods.
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PolarisOpenDictService.class);
    private static final Marker OPENDIC_MARKER = MarkerFactory.getMarker("OPENDIC");
    private static final Namespace NAMESPACE = Namespace.of("SYSTEM");
    private static final Namespace PLATFORM_MAPPINGS_NAMESPACE = Namespace.of("SYSTEM", "PLATFORM_MAPPINGS");
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

    public String defineSchema(UserDefinedEntitySchema schema) throws AlreadyExistsException {
        return icebergRepository.createTable(NAMESPACE, schema.typeName(), UserDefinedEntitySchema.getIcebergSchema(schema));
    }

    public Map<String, String> listUdoTypes(RealmContext realmContext, SecurityContext securityContext) {
        return icebergRepository.listTablesAsStringMap(NAMESPACE);
    }

    public String createUdo(UserDefinedEntity entity) throws IOException, AlreadyExistsException {
        var schema = icebergRepository.readTableSchema(NAMESPACE, entity.typeName());
        var genericRecord = icebergRepository.createGenericRecord(schema, entity.toMap());
        icebergRepository.insertRecord(NAMESPACE, entity.typeName(), genericRecord);
        return genericRecord.toString();
    }

    public List<String> listUdosOfType(String typeName) {
        return icebergRepository.readRecords(NAMESPACE, typeName)
                .stream()
                .map(Record::toString)
                .toList();
    }

    public boolean deleteUdoOfType(String typeName) {
        return icebergRepository.dropTable(NAMESPACE, typeName);
    }


    public String createPlatformMapping(UserDefinedPlatformMapping mappingEntity) throws IOException {
        var schema = mappingEntity.icebergSchema();
        icebergRepository.createTableIfNotExists(PLATFORM_MAPPINGS_NAMESPACE, mappingEntity.platformName(), schema);
        var genericRecord = mappingEntity.toGenericRecord();
        icebergRepository.insertRecord(PLATFORM_MAPPINGS_NAMESPACE, mappingEntity.platformName(), genericRecord);
        return genericRecord.toString();
    }

    public boolean deleteMappingsForPlatform(String platformName) {
        return icebergRepository.dropTable(PLATFORM_MAPPINGS_NAMESPACE, platformName);
    }

    public List<String> listPlatforms(RealmContext realmContext, SecurityContext securityContext) {
        return icebergRepository.listTablesAsStringMap(PLATFORM_MAPPINGS_NAMESPACE)
                .keySet().stream().toList();
    }

    public List<String> listMappingsForPlatform(String platformName) {
        return icebergRepository.readRecords(PLATFORM_MAPPINGS_NAMESPACE, platformName)
                .stream()
                .map(Record::toString)
                .toList();
    }
}
