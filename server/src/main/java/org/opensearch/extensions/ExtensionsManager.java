/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.ActionModule;
import org.opensearch.action.ActionModule.DynamicActionRegistry;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.ClusterSettingsResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.core.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.transport.TransportAddress;

import org.opensearch.discovery.InitializeExtensionRequest;
import org.opensearch.discovery.InitializeExtensionResponse;
import org.opensearch.extensions.ExtensionsSettings.Extension;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.extensions.action.ExtensionTransportActionsHandler;
import org.opensearch.extensions.action.RegisterTransportActionsRequest;
import org.opensearch.extensions.action.RemoteExtensionActionResponse;
import org.opensearch.extensions.action.TransportActionRequestFromExtension;
import org.opensearch.extensions.rest.RegisterRestActionsRequest;
import org.opensearch.extensions.rest.RestActionsRequestHandler;
import org.opensearch.extensions.settings.CustomSettingsRequestHandler;
import org.opensearch.extensions.settings.RegisterCustomSettingsRequest;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;
import org.opensearch.env.EnvironmentSettingsResponse;

/**
 * The main class for managing Extension communication with the OpenSearch Node.
 *
 * @opensearch.internal
 */
public class ExtensionsManager {
    public static final String REQUEST_EXTENSION_ACTION_NAME = "internal:discovery/extensions";
    public static final String REQUEST_EXTENSION_CLUSTER_STATE = "internal:discovery/clusterstate";
    public static final String REQUEST_EXTENSION_CLUSTER_SETTINGS = "internal:discovery/clustersettings";
    public static final String REQUEST_EXTENSION_ENVIRONMENT_SETTINGS = "internal:discovery/enviornmentsettings";
    public static final String REQUEST_EXTENSION_ADD_SETTINGS_UPDATE_CONSUMER = "internal:discovery/addsettingsupdateconsumer";
    public static final String REQUEST_EXTENSION_UPDATE_SETTINGS = "internal:discovery/updatesettings";
    public static final String REQUEST_EXTENSION_DEPENDENCY_INFORMATION = "internal:discovery/dependencyinformation";
    public static final String REQUEST_EXTENSION_REGISTER_CUSTOM_SETTINGS = "internal:discovery/registercustomsettings";
    public static final String REQUEST_EXTENSION_REGISTER_REST_ACTIONS = "internal:discovery/registerrestactions";
    public static final String REQUEST_EXTENSION_REGISTER_TRANSPORT_ACTIONS = "internal:discovery/registertransportactions";
    public static final String REQUEST_REST_EXECUTE_ON_EXTENSION_ACTION = "internal:extensions/restexecuteonextensiontaction";
    public static final String REQUEST_EXTENSION_HANDLE_TRANSPORT_ACTION = "internal:extensions/handle-transportaction";
    public static final String REQUEST_EXTENSION_HANDLE_REMOTE_TRANSPORT_ACTION = "internal:extensions/handle-remote-transportaction";
    public static final String TRANSPORT_ACTION_REQUEST_FROM_EXTENSION = "internal:extensions/request-transportaction-from-extension";
    public static final int EXTENSION_REQUEST_WAIT_TIMEOUT = 10;
    private static final Logger logger = LogManager.getLogger(ExtensionsManager.class);

    /**
     * Enum for OpenSearch Requests
     *
     * @opensearch.internal
     */
    public static enum OpenSearchRequestType {
        REQUEST_OPENSEARCH_NAMED_WRITEABLE_REGISTRY
    }

    private ExtensionTransportActionsHandler extensionTransportActionsHandler;
    private Map<String, Extension> extensionSettingsMap;
    private Map<String, DiscoveryExtensionNode> initializedExtensions;
    private Map<String, DiscoveryExtensionNode> extensionIdMap;
    private RestActionsRequestHandler restActionsRequestHandler;
    private CustomSettingsRequestHandler customSettingsRequestHandler;
    private TransportService transportService;
    private ClusterService clusterService;
    private final Set<Setting<?>> additionalSettings;
    private ExecutorService service;
    private Settings environmentSettings;
    private AddSettingsUpdateConsumerRequestHandler addSettingsUpdateConsumerRequestHandler;
    private NodeClient client;

    /**
     * Instantiate a new ExtensionsManager object to handle requests and responses from extensions. This is called during Node bootstrap.
     *
     * @param additionalSettings  Additional settings to read in from extensions.yml
     * @throws IOException  If the extensions discovery file is not properly retrieved.
     */
    public ExtensionsManager(Set<Setting<?>> additionalSettings) throws IOException {
        logger.info("ExtensionsManager initialized");
        this.initializedExtensions = new HashMap<String, DiscoveryExtensionNode>();
        this.extensionIdMap = new HashMap<String, DiscoveryExtensionNode>();
        this.extensionSettingsMap = new HashMap<String, Extension>();
        // will be initialized in initializeServicesAndRestHandler which is called after the Node is initialized
        this.transportService = null;
        this.clusterService = null;
        // Settings added to extensions.yml by ExtensionAwarePlugins, such as security settings
        this.additionalSettings = new HashSet<>();
        if (additionalSettings != null) {
            this.additionalSettings.addAll(additionalSettings);
        }
        this.client = null;
        this.extensionTransportActionsHandler = null;
    }

    /**
     * Initializes the {@link RestActionsRequestHandler}, {@link TransportService}, {@link ClusterService} and environment settings. This is called during Node bootstrap.
     * Lists/maps of extensions have already been initialized but not yet populated.
     *
     * @param actionModule The ActionModule with the RestController and DynamicActionModule
     * @param settingsModule The module that binds the provided settings to interface.
     * @param transportService  The Node's transport service.
     * @param clusterService  The Node's cluster service.
     * @param initialEnvironmentSettings The finalized view of settings for the Environment
     * @param client The client used to make transport requests
     */
    public void initializeServicesAndRestHandler(
        ActionModule actionModule,
        SettingsModule settingsModule,
        TransportService transportService,
        ClusterService clusterService,
        Settings initialEnvironmentSettings,
        NodeClient client
    ) {
        this.restActionsRequestHandler = new RestActionsRequestHandler(actionModule.getRestController(), extensionIdMap, transportService);
        this.customSettingsRequestHandler = new CustomSettingsRequestHandler(settingsModule);
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.environmentSettings = initialEnvironmentSettings;
        this.addSettingsUpdateConsumerRequestHandler = new AddSettingsUpdateConsumerRequestHandler(
            clusterService,
            transportService,
            REQUEST_EXTENSION_UPDATE_SETTINGS
        );
        this.client = client;
        this.extensionTransportActionsHandler = new ExtensionTransportActionsHandler(
            extensionIdMap,
            transportService,
            client,
            actionModule,
            this
        );
        registerRequestHandler(actionModule.getDynamicActionRegistry());
    }

    /**
     * Lookup an initialized extension by its unique id
     *
     * @param extensionId The unique extension identifier
     * @return An optional of the DiscoveryExtensionNode instance for the matching extension
     */
    public Optional<DiscoveryExtensionNode> lookupInitializedExtensionById(final String extensionId) {
        return Optional.ofNullable(this.initializedExtensions.get(extensionId));
    }

    /**
     * Lookup the settings for an extension based on unique id for the settings placed in extensions.yml
     *
     * @param extensionId The unique extension identifier
     * @return An optional of the Extension instance for the matching extension
     */
    public Optional<Extension> lookupExtensionSettingsById(final String extensionId) {
        return Optional.ofNullable(this.extensionSettingsMap.get(extensionId));
    }

    /**
     * Handles Transport Request from {@link org.opensearch.extensions.action.ExtensionTransportAction} which was invoked by an extension via {@link ExtensionTransportActionsHandler}.
     *
     * @param request which was sent by an extension.
     */
    public RemoteExtensionActionResponse handleRemoteTransportRequest(ExtensionActionRequest request) throws Exception {
        return extensionTransportActionsHandler.sendRemoteTransportRequestToExtension(request);
    }

    /**
     * Handles Transport Request from {@link org.opensearch.extensions.action.ExtensionTransportAction} which was invoked by OpenSearch or a plugin
     *
     * @param request which was sent by an extension.
     */
    public ExtensionActionResponse handleTransportRequest(ExtensionActionRequest request) throws Exception {
        return extensionTransportActionsHandler.sendTransportRequestToExtension(request);
    }

    private void registerRequestHandler(DynamicActionRegistry dynamicActionRegistry) {
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_REST_ACTIONS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterRestActionsRequest::new,
            ((request, channel, task) -> channel.sendResponse(
                restActionsRequestHandler.handleRegisterRestActionsRequest(request, dynamicActionRegistry)
            ))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_CUSTOM_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterCustomSettingsRequest::new,
            ((request, channel, task) -> channel.sendResponse(customSettingsRequestHandler.handleRegisterCustomSettingsRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_CLUSTER_STATE,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_CLUSTER_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_ENVIRONMENT_SETTINGS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_DEPENDENCY_INFORMATION,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ExtensionRequest::new,
            ((request, channel, task) -> channel.sendResponse(handleExtensionRequest(request)))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_ADD_SETTINGS_UPDATE_CONSUMER,
            ThreadPool.Names.GENERIC,
            false,
            false,
            AddSettingsUpdateConsumerRequest::new,
            ((request, channel, task) -> channel.sendResponse(
                addSettingsUpdateConsumerRequestHandler.handleAddSettingsUpdateConsumerRequest(request)
            ))
        );
        transportService.registerRequestHandler(
            REQUEST_EXTENSION_REGISTER_TRANSPORT_ACTIONS,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RegisterTransportActionsRequest::new,
            ((request, channel, task) -> channel.sendResponse(
                extensionTransportActionsHandler.handleRegisterTransportActionsRequest(request)
            ))
        );
        transportService.registerRequestHandler(
            TRANSPORT_ACTION_REQUEST_FROM_EXTENSION,
            ThreadPool.Names.GENERIC,
            false,
            false,
            TransportActionRequestFromExtension::new,
            ((request, channel, task) -> channel.sendResponse(
                extensionTransportActionsHandler.handleTransportActionRequestFromExtension(request)
            ))
        );
    }

    /**
     * Loads a single extension
     * @param extension The extension to be loaded
     */
    public String loadExtension(Extension extension) throws IOException {
        try {
            validateExtension(extension);
            DiscoveryExtensionNode discoveryExtensionNode = new DiscoveryExtensionNode(
                extension.getName(),
                extension.getUniqueId(),
                new TransportAddress(InetAddress.getByName(extension.getHostAddress()), Integer.parseInt(extension.getPort())),
                new HashMap<String, String>(),
                Version.fromString(extension.getOpensearchVersion()),
                Version.fromString(extension.getMinimumCompatibleVersion()),
                extension.getDependencies()
            );
            extensionIdMap.put(extension.getUniqueId(), discoveryExtensionNode);
            extensionSettingsMap.put(extension.getUniqueId(), extension);
            logger.info("Loaded extension with uniqueId " + extension.getUniqueId() + ": " + extension);
        } catch (IOException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw e;
        }
        return extension.getUniqueId();

    }

    private boolean validateExtension(Extension extension) throws IOException {
        if (Strings.isNullOrEmpty(extension.getName())) {
            throw new IOException("Required field [name] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getUniqueId())) {
            throw new IOException("Required field [uniqueId] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getHostAddress())) {
            throw new IOException("Required field [extension host address] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getPort())) {
            throw new IOException("Required field [extension port] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getVersion())) {
            throw new IOException("Required field [extension version] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getOpensearchVersion())) {
            throw new IOException("Required field [opensearch version] is missing in the request");
        } else if (Strings.isNullOrEmpty(extension.getMinimumCompatibleVersion())) {
            throw new IOException("Required field [minimum opensearch version] is missing in the request");
        } else if (extensionIdMap.containsKey(extension.getUniqueId())) {
            throw new IOException("Duplicate uniqueId " + extension.getUniqueId() + ". Did not load extension: " + extension);
        }
        return true;
    }

    /**
     * Iterate through all extensions and initialize them.  Initialized extensions will be added to the {@link #initializedExtensions}.
     */
    public void initialize(String extensionId) throws Exception {
        /*List<CompletableFuture<?>> futures = new ArrayList<>();
        for (DiscoveryExtensionNode extension : extensionIdMap.values()) {
            CompletableFuture<?> future = initializeExtension(extension);
//            CompletableFuture.allOf(future).join();
            futures.add(future);
        }*/
        DiscoveryExtensionNode extensionNode = extensionIdMap.get(extensionId);
        CompletableFuture<?> future = initializeExtension(extensionNode);

        CompletableFuture.allOf(future).join();



       // getAllCompleted(futures, EXTENSION_REQUEST_WAIT_TIMEOUT + 1, TimeUnit.SECONDS);
        service.shutdown();
        service.awaitTermination(EXTENSION_REQUEST_WAIT_TIMEOUT + 1, TimeUnit.SECONDS);
        //return futures;
    }

    public List<?> getAllCompleted(List<CompletableFuture<?>> futuresList, long timeout, TimeUnit unit) {
        CompletableFuture<Void> allFuturesResult = CompletableFuture.allOf(futuresList.toArray(new CompletableFuture[futuresList.size()]));
        try {
            allFuturesResult.get(timeout, unit);
        } catch (Exception e) {
            // you may log it
        }

        CompletableFuture.allOf(allFuturesResult).join();
        return futuresList
            .stream()
            .filter(future -> future.isDone() && !future.isCompletedExceptionally()) // keep only the ones completed
            .map(CompletableFuture::join) // get the value from the completed future
            .collect(Collectors.toList()); // collect as a list*/
    }

    private CompletableFuture initializeExtension(DiscoveryExtensionNode extension) throws Exception {

        final CompletableFuture<InitializeExtensionResponse> inProgressFuture = new CompletableFuture<>();
        final TransportResponseHandler<InitializeExtensionResponse> initializeExtensionResponseHandler = new TransportResponseHandler<
            InitializeExtensionResponse>() {

            @Override
            public InitializeExtensionResponse read(StreamInput in) throws IOException {
                return new InitializeExtensionResponse(in);
            }

            @Override
            public void handleResponse(InitializeExtensionResponse response) {
                for (DiscoveryExtensionNode extension : extensionIdMap.values()) {
                    if (extension.getName().equals(response.getName())) {
                        extension.setImplementedInterfaces(response.getImplementedInterfaces());
                        initializedExtensions.put(extension.getId(), extension);
                        logger.info("Initialized extension: " + extension.getName());
                        break;
                    }
                }
                inProgressFuture.complete(response);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.error(new ParameterizedMessage("Extension initialization failed"), exp);
                inProgressFuture.completeExceptionally(exp);
                throw exp;
            }

            @Override
            public String executor() {
                return ThreadPool.Names.GENERIC;
            }
        };

        logger.info("Sending extension request type: " + REQUEST_EXTENSION_ACTION_NAME);
        service = transportService.getThreadPool().generic();
        service.submit(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                if (inProgressFuture.completeExceptionally(e)) {
                    extensionIdMap.remove(extension.getId());
                    logger.info("Connection Error!", e);
                    if (e.getCause() instanceof ConnectTransportException) {
                        logger.info("No response from extension to request.", e);
                        throw (ConnectTransportException) e.getCause();
                    } else if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else if (e.getCause() instanceof Error) {
                        throw (Error) e.getCause();
                    } else {
                        throw new RuntimeException(e.getCause());
                    }
                }
            }

            @Override
            protected void doRun() throws Exception {
                transportService.connectToExtensionNode(extension);
                transportService.sendRequest(
                    extension,
                    REQUEST_EXTENSION_ACTION_NAME,
                    new InitializeExtensionRequest(transportService.getLocalNode(), extension),
                    initializeExtensionResponseHandler
                );
                inProgressFuture.orTimeout(EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
//                inProgressFuture.complete(null);
            }
        });







        // Create completablefuture
//        final CompletableFuture<AcknowledgedResponse> extensionResponseFuture = new CompletableFuture<>();
       // ExecutorService service = transportService.getThreadPool().generic();
       /* Future<?> future = service.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    transportService.connectToExtensionNode(extension);
                    transportService.sendRequest(
                        extension,
                        REQUEST_EXTENSION_ACTION_NAME,
                        new InitializeExtensionRequest(transportService.getLocalNode(), extension),
                        initializeExtensionResponseHandler
                    );
                    //complete future
                    inProgressFuture.orTimeout(EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
                } catch (Exception e) {
                    if (e instanceof ConnectTransportException) {
                        logger.info("No response from extension to request.", e);
                        throw (ConnectTransportException) e.getCause();
                    }
                    else if (e.getCause() instanceof RuntimeException) {
                        throw (RuntimeException) e.getCause();
                    } else if (e.getCause() instanceof Error) {
                        throw (Error) e.getCause();
                    } else {
                        throw new RuntimeException(e.getCause());
                    }
                }
            }
            boolean serviceDone = service.awaitTermination(EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS);
        });*/


       /* Future<?> future = service.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                logger.info("Before connectToNodeExtenionNode");
                    transportService.connectToExtensionNode(extension);
                logger.info("After connectToNodeExtenionNode");
                    transportService.sendRequest(
                        extension,
                        REQUEST_EXTENSION_ACTION_NAME,
                        new InitializeExtensionRequest(transportService.getLocalNode(), extension),
                        initializeExtensionResponseHandler
                    );
                    //complete future
                    inProgressFuture.orTimeout(EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
                return null;
            }
        });

        try {
            future.get();
        } catch (ExecutionException e) {
            if (e instanceof ExecutionException) {
                logger.info("No response from extension to request.", e);
                throw (ConnectTransportException) e.getCause();
            }
            else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            } else {
                throw new RuntimeException(e.getCause());
            }
        }

        service.shutdown();*/



//        Future<?> future = service.submit(() -> {



//        service.awaitTermination(EXTENSION_REQUEST_WAIT_TIMEOUT + 1, TimeUnit.SECONDS);
        //timeout in progress
//        if (inProgressFuture.isCompletedExceptionally()) {
//            throw new RuntimeException("Connection interupted!");
//        }

//        extensionResponseFuture.orTimeout(EXTENSION_REQUEST_WAIT_TIMEOUT, TimeUnit.SECONDS).join();
    //return future;

        return inProgressFuture;
    }

    /**
     * Handles an {@link ExtensionRequest}.
     *
     * @param extensionRequest  The request to handle, of a type defined in the {@link org.opensearch.extensions.proto.ExtensionRequestProto.RequestType} enum.
     * @return  an Response matching the request.
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleExtensionRequest(ExtensionRequest extensionRequest) throws Exception {
        switch (extensionRequest.getRequestType()) {
            case REQUEST_EXTENSION_CLUSTER_STATE:
                return new ClusterStateResponse(clusterService.getClusterName(), clusterService.state(), false);
            case REQUEST_EXTENSION_CLUSTER_SETTINGS:
                return new ClusterSettingsResponse(clusterService);
            case REQUEST_EXTENSION_ENVIRONMENT_SETTINGS:
                return new EnvironmentSettingsResponse(this.environmentSettings);
            case REQUEST_EXTENSION_DEPENDENCY_INFORMATION:
                String uniqueId = extensionRequest.getUniqueId();
                if (uniqueId == null) {
                    return new ExtensionDependencyResponse(
                        initializedExtensions.entrySet().stream().map(e -> e.getValue()).collect(Collectors.toList())
                    );
                } else {
                    ExtensionDependency matchingId = new ExtensionDependency(uniqueId, Version.CURRENT);
                    return new ExtensionDependencyResponse(
                        initializedExtensions.entrySet()
                            .stream()
                            .map(e -> e.getValue())
                            .filter(e -> e.dependenciesContain(matchingId))
                            .collect(Collectors.toList())
                    );
                }
            default:
                throw new IllegalArgumentException("Handler not present for the provided request");
        }
    }

    static String getRequestExtensionActionName() {
        return REQUEST_EXTENSION_ACTION_NAME;
    }

    static String getRequestExtensionClusterState() {
        return REQUEST_EXTENSION_CLUSTER_STATE;
    }

    static String getRequestExtensionClusterSettings() {
        return REQUEST_EXTENSION_CLUSTER_SETTINGS;
    }

    static Logger getLogger() {
        return logger;
    }

    TransportService getTransportService() {
        return transportService;
    }

    ClusterService getClusterService() {
        return clusterService;
    }

    Map<String, DiscoveryExtensionNode> getExtensionIdMap() {
        return extensionIdMap;
    }

    RestActionsRequestHandler getRestActionsRequestHandler() {
        return restActionsRequestHandler;
    }

    void setExtensionIdMap(Map<String, DiscoveryExtensionNode> extensionIdMap) {
        this.extensionIdMap = extensionIdMap;
    }

    void setRestActionsRequestHandler(RestActionsRequestHandler restActionsRequestHandler) {
        this.restActionsRequestHandler = restActionsRequestHandler;
    }

    void setTransportService(TransportService transportService) {
        this.transportService = transportService;
    }

    void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    CustomSettingsRequestHandler getCustomSettingsRequestHandler() {
        return customSettingsRequestHandler;
    }

    void setCustomSettingsRequestHandler(CustomSettingsRequestHandler customSettingsRequestHandler) {
        this.customSettingsRequestHandler = customSettingsRequestHandler;
    }

    AddSettingsUpdateConsumerRequestHandler getAddSettingsUpdateConsumerRequestHandler() {
        return addSettingsUpdateConsumerRequestHandler;
    }

    void setAddSettingsUpdateConsumerRequestHandler(AddSettingsUpdateConsumerRequestHandler addSettingsUpdateConsumerRequestHandler) {
        this.addSettingsUpdateConsumerRequestHandler = addSettingsUpdateConsumerRequestHandler;
    }

    Settings getEnvironmentSettings() {
        return environmentSettings;
    }
}
