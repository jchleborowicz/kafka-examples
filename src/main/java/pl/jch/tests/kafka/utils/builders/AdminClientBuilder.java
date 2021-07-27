package pl.jch.tests.kafka.utils.builders;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Sensor;
import pl.jch.tests.kafka.utils.functions.CheckedConsumer;

import static pl.jch.tests.kafka.utils.functions.CheckedExceptionUtils.wrapCheckedConsumer;

@SuppressWarnings("unused")
public final class AdminClientBuilder {

    private final Map<String, Object> properties = new HashMap<>();

    private AdminClientBuilder() {
    }

    public static AdminClientBuilder builder() {
        return new AdminClientBuilder()
                .bootstrapServers(BuilderConstants.DEFAULT_BOOTSTRAP_SERVERS);
    }

    private AdminClient build() {
        return AdminClient.create(properties);
    }

    public void buildAndExecute(CheckedConsumer<AdminClient> callback) {
        try (AdminClient adminClient = this.build()) {
            wrapCheckedConsumer(callback)
                    .accept(adminClient);
        }
    }

    public AdminClientBuilder config(String configName, Object value) {
        this.properties.put(configName, value);
        return this;
    }

    // ### AUTOGENERATED BUILDER METHODS START ###
    // DO NOT EDIT MANUALLY

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The
     * client will make use of all servers irrespective of which servers are specified here for
     * bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of
     * servers. This list should be in the form <code>host1:port1,host2:port2,...</code>. Since these
     * servers are just used for the initial connection to discover the full cluster membership (which may
     * change dynamically), this list need not contain the full set of servers (you may want more than one,
     * though, in case a server is down).
     */
    public AdminClientBuilder bootstrapServers(String bootstrapServers) {
        return config(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * Controls how the client uses DNS lookups. If set to <code>use_all_dns_ips</code>, connect to each
     * returned IP address in sequence until a successful connection is established. After a disconnection,
     * the next IP is used. Once all IPs have been used once, the client resolves the IP(s) from the
     * hostname again (both the JVM and the OS cache DNS name lookups, however). If set to
     * <code>resolve_canonical_bootstrap_servers_only</code>, resolve each bootstrap address into a list of
     * canonical names. After the bootstrap phase, this behaves the same as <code>use_all_dns_ips</code>.
     * If set to <code>default</code> (deprecated), attempt to connect to the first IP address returned by
     * the lookup, even if the lookup returns multiple IP addresses.
     */
    public AdminClientBuilder clientDnsLookup(ClientDnsLookup clientDnsLookup) {
        return config(AdminClientConfig.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup);
    }

    /**
     * An id string to pass to the server when making requests. The purpose of this is to be able to track
     * the source of requests beyond just ip/port by allowing a logical application name to be included in
     * server-side request logging.
     */
    public AdminClientBuilder clientId(String clientId) {
        return config(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
    }

    /**
     * Close idle connections after the number of milliseconds specified by this config.
     */
    public AdminClientBuilder connectionsMaxIdleMs(long connectionsMaxIdleMs) {
        return config(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
    }

    /**
     * Specifies the timeout (in milliseconds) for client APIs. This configuration is used as the default
     * timeout for all client operations that do not specify a <code>timeout</code> parameter.
     */
    public AdminClientBuilder defaultApiTimeoutMs(int defaultApiTimeoutMs) {
        return config(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
    }

    /**
     * The period of time in milliseconds after which we force a refresh of metadata even if we haven't
     * seen any partition leadership changes to proactively discover any new brokers or partitions.
     */
    public AdminClientBuilder metadataMaxAgeMs(long metadataMaxAgeMs) {
        return config(AdminClientConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs);
    }

    /**
     * A list of classes to use as metrics reporters. Implementing the
     * <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes
     * that will be notified of new metric creation. The JmxReporter is always included to register JMX
     * statistics.
     */
    public AdminClientBuilder metricReporters(String metricReporters) {
        return config(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, metricReporters);
    }

    /**
     * The number of samples maintained to compute metrics.
     */
    public AdminClientBuilder metricsNumSamples(int metricsNumSamples) {
        return config(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
    }

    /**
     * The highest recording level for metrics.
     */
    public AdminClientBuilder metricsRecordingLevel(Sensor.RecordingLevel metricsRecordingLevel) {
        return config(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel);
    }

    /**
     * The window of time a metrics sample is computed over.
     */
    public AdminClientBuilder metricsSampleWindowMs(long metricsSampleWindowMs) {
        return config(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindowMs);
    }

    /**
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS
     * default will be used.
     */
    public AdminClientBuilder receiveBufferBytes(int receiveBufferBytes) {
        return config(AdminClientConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
    }

    /**
     * The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly
     * failed to connect. If provided, the backoff per host will increase exponentially for each
     * consecutive connection failure, up to this maximum. After calculating the backoff increase, 20%
     * random jitter is added to avoid connection storms.
     */
    public AdminClientBuilder reconnectBackoffMaxMs(long reconnectBackoffMaxMs) {
        return config(AdminClientConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMaxMs);
    }

    /**
     * The base amount of time to wait before attempting to reconnect to a given host. This avoids
     * repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by
     * the client to a broker.
     */
    public AdminClientBuilder reconnectBackoffMs(long reconnectBackoffMs) {
        return config(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
    }

    /**
     * The configuration controls the maximum amount of time the client will wait for the response of a
     * request. If the response is not received before the timeout elapses the client will resend the
     * request if necessary or fail the request if retries are exhausted.
     */
    public AdminClientBuilder requestTimeoutMs(int requestTimeoutMs) {
        return config(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    }

    /**
     * Setting a value greater than zero will cause the client to resend any request that fails with a
     * potentially transient error. It is recommended to set the value to either zero or `MAX_VALUE` and
     * use corresponding timeout parameters to control how long a client should retry a request.
     */
    public AdminClientBuilder retries(int retries) {
        return config(AdminClientConfig.RETRIES_CONFIG, retries);
    }

    /**
     * The amount of time to wait before attempting to retry a failed request. This avoids repeatedly
     * sending requests in a tight loop under some failure scenarios.
     */
    public AdminClientBuilder retryBackoffMs(long retryBackoffMs) {
        return config(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
    }

    /**
     * The fully qualified name of a SASL client callback handler class that implements the
     * AuthenticateCallbackHandler interface.
     */
    public AdminClientBuilder saslClientCallbackHandlerClass(Class<?> saslClientCallbackHandlerClass) {
        return config(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, saslClientCallbackHandlerClass);
    }

    /**
     * JAAS login context parameters for SASL connections in the format used by JAAS configuration files.
     * JAAS configuration file format is described <a
     * href="http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html">here</a>.
     * The format for the value is: <code>loginModuleClass controlFlag (optionName=optionValue)*;</code>.
     * For brokers, the config must be prefixed with listener prefix and SASL mechanism name in lower-case.
     * For example, listener.name.sasl_ssl.scram-sha-256.sasl.jaas.config=com.example.ScramLoginModule
     * required;
     */
    public AdminClientBuilder saslJaasConfig(String saslJaasConfig) {
        return config(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    }

    /**
     * Kerberos kinit command path.
     */
    public AdminClientBuilder saslKerberosKinitCmd(String saslKerberosKinitCmd) {
        return config(SaslConfigs.SASL_KERBEROS_KINIT_CMD, saslKerberosKinitCmd);
    }

    /**
     * Login thread sleep time between refresh attempts.
     */
    public AdminClientBuilder saslKerberosMinTimeBeforeRelogin(long saslKerberosMinTimeBeforeRelogin) {
        return config(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, saslKerberosMinTimeBeforeRelogin);
    }

    /**
     * The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or
     * in Kafka's config.
     */
    public AdminClientBuilder saslKerberosServiceName(String saslKerberosServiceName) {
        return config(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
    }

    /**
     * Percentage of random jitter added to the renewal time.
     */
    public AdminClientBuilder saslKerberosTicketRenewJitter(double saslKerberosTicketRenewJitter) {
        return config(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, saslKerberosTicketRenewJitter);
    }

    /**
     * Login thread will sleep until the specified window factor of time from last refresh to ticket's
     * expiry has been reached, at which time it will try to renew the ticket.
     */
    public AdminClientBuilder saslKerberosTicketRenewWindowFactor(double saslKerberosTicketRenewWindowFactor) {
        return config(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, saslKerberosTicketRenewWindowFactor);
    }

    /**
     * The fully qualified name of a SASL login callback handler class that implements the
     * AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed
     * with listener prefix and SASL mechanism name in lower-case. For example,
     * listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler
     */
    public AdminClientBuilder saslLoginCallbackHandlerClass(Class<?> saslLoginCallbackHandlerClass) {
        return config(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, saslLoginCallbackHandlerClass);
    }

    /**
     * The fully qualified name of a class that implements the Login interface. For brokers, login config
     * must be prefixed with listener prefix and SASL mechanism name in lower-case. For example,
     * listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin
     */
    public AdminClientBuilder saslLoginClass(Class<?> saslLoginClass) {
        return config(SaslConfigs.SASL_LOGIN_CLASS, saslLoginClass);
    }

    /**
     * The amount of buffer time before credential expiration to maintain when refreshing a credential, in
     * seconds. If a refresh would otherwise occur closer to expiration than the number of buffer seconds
     * then the refresh will be moved up to maintain as much of the buffer time as possible. Legal values
     * are between 0 and 3600 (1 hour); a default value of  300 (5 minutes) is used if no value is
     * specified. This value and sasl.login.refresh.min.period.seconds are both ignored if their sum
     * exceeds the remaining lifetime of a credential. Currently applies only to OAUTHBEARER.
     */
    public AdminClientBuilder saslLoginRefreshBufferSeconds(short saslLoginRefreshBufferSeconds) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, saslLoginRefreshBufferSeconds);
    }

    /**
     * The desired minimum time for the login refresh thread to wait before refreshing a credential, in
     * seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used
     * if no value is specified.  This value and  sasl.login.refresh.buffer.seconds are both ignored if
     * their sum exceeds the remaining lifetime of a credential. Currently applies only to OAUTHBEARER.
     */
    public AdminClientBuilder saslLoginRefreshMinPeriodSeconds(short saslLoginRefreshMinPeriodSeconds) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, saslLoginRefreshMinPeriodSeconds);
    }

    /**
     * Login refresh thread will sleep until the specified window factor relative to the credential's
     * lifetime has been reached, at which time it will try to refresh the credential. Legal values are
     * between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used if no value is
     * specified. Currently applies only to OAUTHBEARER.
     */
    public AdminClientBuilder saslLoginRefreshWindowFactor(double saslLoginRefreshWindowFactor) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, saslLoginRefreshWindowFactor);
    }

    /**
     * The maximum amount of random jitter relative to the credential's lifetime that is added to the login
     * refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive; a default value of
     * 0.05 (5%) is used if no value is specified. Currently applies only to OAUTHBEARER.
     */
    public AdminClientBuilder saslLoginRefreshWindowJitter(double saslLoginRefreshWindowJitter) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, saslLoginRefreshWindowJitter);
    }

    /**
     * SASL mechanism used for client connections. This may be any mechanism for which a security provider
     * is available. GSSAPI is the default mechanism.
     */
    public AdminClientBuilder saslMechanism(String saslMechanism) {
        return config(SaslConfigs.SASL_MECHANISM, saslMechanism);
    }

    /**
     * Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT,
     * SASL_SSL.
     */
    public AdminClientBuilder securityProtocol(String securityProtocol) {
        return config(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }

    /**
     * A list of configurable creator classes each returning a provider implementing security algorithms.
     * These classes should implement the
     * <code>org.apache.kafka.common.security.auth.SecurityProviderCreator</code> interface.
     */
    public AdminClientBuilder securityProviders(String securityProviders) {
        return config(AdminClientConfig.SECURITY_PROVIDERS_CONFIG, securityProviders);
    }

    /**
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS
     * default will be used.
     */
    public AdminClientBuilder sendBufferBytes(int sendBufferBytes) {
        return config(AdminClientConfig.SEND_BUFFER_CONFIG, sendBufferBytes);
    }

    /**
     * The maximum amount of time the client will wait for the socket connection to be established. The
     * connection setup timeout will increase exponentially for each consecutive connection failure up to
     * this maximum. To avoid connection storms, a randomization factor of 0.2 will be applied to the
     * timeout resulting in a random range between 20% below and 20% above the computed value.
     */
    public AdminClientBuilder socketConnectionSetupTimeoutMaxMs(long socketConnectionSetupTimeoutMaxMs) {
        return config(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, socketConnectionSetupTimeoutMaxMs);
    }

    /**
     * The amount of time the client will wait for the socket connection to be established. If the
     * connection is not built before the timeout elapses, clients will close the socket channel.
     */
    public AdminClientBuilder socketConnectionSetupTimeoutMs(long socketConnectionSetupTimeoutMs) {
        return config(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, socketConnectionSetupTimeoutMs);
    }

    /**
     * A list of cipher suites. This is a named combination of authentication, encryption, MAC and key
     * exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL
     * network protocol. By default all the available cipher suites are supported.
     */
    public AdminClientBuilder sslCipherSuites(String sslCipherSuites) {
        return config(SslConfigs.SSL_CIPHER_SUITES_CONFIG, sslCipherSuites);
    }

    /**
     * The list of protocols enabled for SSL connections. The default is 'TLSv1.2,TLSv1.3' when running
     * with Java 11 or newer, 'TLSv1.2' otherwise. With the default value for Java 11, clients and servers
     * will prefer TLSv1.3 if both support it and fallback to TLSv1.2 otherwise (assuming both support at
     * least TLSv1.2). This default should be fine for most cases. Also see the config documentation for
     * `ssl.protocol`.
     */
    public AdminClientBuilder sslEnabledProtocols(String sslEnabledProtocols) {
        return config(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, sslEnabledProtocols);
    }

    /**
     * The endpoint identification algorithm to validate server hostname using server certificate.
     */
    public AdminClientBuilder sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        return config(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
    }

    /**
     * The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine
     * objects. Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
     */
    public AdminClientBuilder sslEngineFactoryClass(Class<?> sslEngineFactoryClass) {
        return config(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, sslEngineFactoryClass);
    }

    /**
     * The password of the private key in the key store file orthe PEM key specified in `ssl.keystore.key'.
     * This is required for clients only if two-way authentication is configured.
     */
    public AdminClientBuilder sslKeyPassword(String sslKeyPassword) {
        return config(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
    }

    /**
     * The algorithm used by key manager factory for SSL connections. Default value is the key manager
     * factory algorithm configured for the Java Virtual Machine.
     */
    public AdminClientBuilder sslKeymanagerAlgorithm(String sslKeymanagerAlgorithm) {
        return config(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, sslKeymanagerAlgorithm);
    }

    /**
     * Certificate chain in the format specified by 'ssl.keystore.type'. Default SSL engine factory
     * supports only PEM format with a list of X.509 certificates
     */
    public AdminClientBuilder sslKeystoreCertificateChain(String sslKeystoreCertificateChain) {
        return config(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, sslKeystoreCertificateChain);
    }

    /**
     * Private key in the format specified by 'ssl.keystore.type'. Default SSL engine factory supports only
     * PEM format with PKCS#8 keys. If the key is encrypted, key password must be specified using
     * 'ssl.key.password'
     */
    public AdminClientBuilder sslKeystoreKey(String sslKeystoreKey) {
        return config(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, sslKeystoreKey);
    }

    /**
     * The location of the key store file. This is optional for client and can be used for two-way
     * authentication for client.
     */
    public AdminClientBuilder sslKeystoreLocation(String sslKeystoreLocation) {
        return config(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
    }

    /**
     * The store password for the key store file. This is optional for client and only needed if
     * 'ssl.keystore.location' is configured.  Key store password is not supported for PEM format.
     */
    public AdminClientBuilder sslKeystorePassword(String sslKeystorePassword) {
        return config(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
    }

    /**
     * The file format of the key store file. This is optional for client.
     */
    public AdminClientBuilder sslKeystoreType(String sslKeystoreType) {
        return config(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, sslKeystoreType);
    }

    /**
     * The SSL protocol used to generate the SSLContext. The default is 'TLSv1.3' when running with Java 11
     * or newer, 'TLSv1.2' otherwise. This value should be fine for most use cases. Allowed values in
     * recent JVMs are 'TLSv1.2' and 'TLSv1.3'. 'TLS', 'TLSv1.1', 'SSL', 'SSLv2' and 'SSLv3' may be
     * supported in older JVMs, but their usage is discouraged due to known security vulnerabilities. With
     * the default value for this config and 'ssl.enabled.protocols', clients will downgrade to 'TLSv1.2'
     * if the server does not support 'TLSv1.3'. If this config is set to 'TLSv1.2', clients will not use
     * 'TLSv1.3' even if it is one of the values in ssl.enabled.protocols and the server only supports
     * 'TLSv1.3'.
     */
    public AdminClientBuilder sslProtocol(String sslProtocol) {
        return config(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
    }

    /**
     * The name of the security provider used for SSL connections. Default value is the default security
     * provider of the JVM.
     */
    public AdminClientBuilder sslProvider(String sslProvider) {
        return config(SslConfigs.SSL_PROVIDER_CONFIG, sslProvider);
    }

    /**
     * The SecureRandom PRNG implementation to use for SSL cryptography operations.
     */
    public AdminClientBuilder sslSecureRandomImplementation(String sslSecureRandomImplementation) {
        return config(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, sslSecureRandomImplementation);
    }

    /**
     * The algorithm used by trust manager factory for SSL connections. Default value is the trust manager
     * factory algorithm configured for the Java Virtual Machine.
     */
    public AdminClientBuilder sslTrustmanagerAlgorithm(String sslTrustmanagerAlgorithm) {
        return config(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, sslTrustmanagerAlgorithm);
    }

    /**
     * Trusted certificates in the format specified by 'ssl.truststore.type'. Default SSL engine factory
     * supports only PEM format with X.509 certificates.
     */
    public AdminClientBuilder sslTruststoreCertificates(String sslTruststoreCertificates) {
        return config(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, sslTruststoreCertificates);
    }

    /**
     * The location of the trust store file.
     */
    public AdminClientBuilder sslTruststoreLocation(String sslTruststoreLocation) {
        return config(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
    }

    /**
     * The password for the trust store file. If a password is not set, trust store file configured will
     * still be used, but integrity checking is disabled. Trust store password is not supported for PEM
     * format.
     */
    public AdminClientBuilder sslTruststorePassword(String sslTruststorePassword) {
        return config(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
    }

    /**
     * The file format of the trust store file.
     */
    public AdminClientBuilder sslTruststoreType(String sslTruststoreType) {
        return config(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreType);
    }

    // ### AUTOGENERATED BUILDER METHODS END ###

}