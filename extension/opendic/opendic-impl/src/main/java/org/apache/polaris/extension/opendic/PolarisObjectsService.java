package org.apache.polaris.extension.opendic;

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.admin.model.CatalogGrant;
import org.apache.polaris.core.admin.model.CatalogPrivilege;
import org.apache.polaris.core.admin.model.GrantResource;
import org.apache.polaris.core.admin.model.NamespaceGrant;
import org.apache.polaris.core.admin.model.NamespacePrivilege;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.core.admin.model.TableGrant;
import org.apache.polaris.core.admin.model.TablePrivilege;
import org.apache.polaris.core.admin.model.UpdateCatalogRequest;
import org.apache.polaris.core.admin.model.UpdateCatalogRoleRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRequest;
import org.apache.polaris.core.admin.model.UpdatePrincipalRoleRequest;
import org.apache.polaris.core.admin.model.ViewGrant;
import org.apache.polaris.core.admin.model.ViewPrivilege;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizableOperation;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisGrantManager.LoadGrantsResult;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.core.entity.CatalogRoleEntity;
import org.apache.polaris.core.entity.NamespaceEntity;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.entity.PrincipalRoleEntity;
import org.apache.polaris.core.entity.TableLikeEntity;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.persistence.dao.entity.CreateCatalogResult;
import org.apache.polaris.core.persistence.dao.entity.CreatePrincipalResult;
import org.apache.polaris.core.persistence.dao.entity.DropEntityResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.resolver.PolarisResolutionManifest;
import org.apache.polaris.core.persistence.resolver.ResolverPath;
import org.apache.polaris.core.persistence.resolver.ResolverStatus;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.AwsStorageConfigurationInfo;
import org.apache.polaris.core.storage.azure.AzureStorageConfigurationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolarisObjectsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolarisObjectsService.class);

    private final CallContext callContext;
    private final PolarisEntityManager entityManager;
    private final SecurityContext securityContext;
    private final AuthenticatedPolarisPrincipal authenticatedPrincipal;
    private final PolarisAuthorizer authorizer;
    private final PolarisMetaStoreManager metaStoreManager;

    // Initialized in the authorize methods.
    private PolarisResolutionManifest resolutionManifest = null;

    public PolarisObjectsService(
        @NotNull CallContext callContext,
        @NotNull PolarisEntityManager entityManager,
        @NotNull PolarisMetaStoreManager metaStoreManager,
        @NotNull SecurityContext securityContext,
        @NotNull PolarisAuthorizer authorizer
    ) {
        this.callContext = callContext;
        this.entityManager = entityManager;
        this.metaStoreManager = metaStoreManager;
        this.securityContext = securityContext;
        PolarisDiagnostics diagServices = callContext.getPolarisCallContext().getDiagServices();
        diagServices.checkNotNull(securityContext, "null_security_context");
        diagServices.checkNotNull(securityContext.getUserPrincipal(), "null_security_context");
        diagServices.check(
            securityContext.getUserPrincipal() instanceof AuthenticatedPolarisPrincipal,
            "unexpected_principal_type",
            "class={}",
            securityContext.getUserPrincipal().getClass().getName()
        );
        this.authenticatedPrincipal = (AuthenticatedPolarisPrincipal) securityContext.getUserPrincipal();
        this.authorizer = authorizer;
    }
}
