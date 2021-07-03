package pl.jch.tests.kafka.utils;


import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@SuppressWarnings("unused")
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConsumerBuilder {

    private final Map<String, Object> properties = new HashMap<>();

    public static ConsumerBuilder builder() {
        return new ConsumerBuilder()
                .bootstrapServers("localhost:9092")
                .schemaRegistryUrl("http://localhost:8081")
                .enableAutoCommit(true)
                .autoCommitIntervalMs(1_000)
                .autoOffsetReset(ConsumerBuilder.AutoOffsetReset.EARLIEST);
    }

    public static ConsumerBuilder builder(Class<StringDeserializer> keyDeserializer,
                                          Class<KafkaAvroDeserializer> valueDeserializer) {
        return builder()
                .config(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer)
                .config(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    }

    public ConsumerBuilder schemaRegistryUrl(String schemaRegistryUrl) {
        return config(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    }

    // ### AUTOGENERATED BUILDER METHODS START ###
    // DO NOT EDIT MANUALLY

    /**
     * Allow automatic topic creation on the broker when subscribing to or assigning a topic. A topic being
     * subscribed to will be automatically created only if the broker allows for it using
     * `auto.create.topics.enable` broker configuration. This configuration must be set to `false` when
     * using brokers older than 0.11.0
     */
    public ConsumerBuilder allowAutoCreateTopics(boolean allowAutoCreateTopics) {
        return config(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopics);
    }

    /**
     * The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if
     * <code>enable.auto.commit</code> is set to <code>true</code>.
     */
    public ConsumerBuilder autoCommitIntervalMs(int autoCommitIntervalMs) {
        return config(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);
    }

    /**
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more
     * on the server (e.g. because that data has been deleted): <ul><li>earliest: automatically reset the
     * offset to the earliest offset<li>latest: automatically reset the offset to the latest
     * offset</li><li>none: throw exception to the consumer if no previous offset is found for the
     * consumer's group</li><li>anything else: throw exception to the consumer.</li></ul>
     */
    public ConsumerBuilder autoOffsetReset(AutoOffsetReset autoOffsetReset) {
        return config(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
    }

    /**
     * A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The
     * client will make use of all servers irrespective of which servers are specified here for
     * bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of
     * servers. This list should be in the form <code>host1:port1,host2:port2,...</code>. Since these
     * servers are just used for the initial connection to discover the full cluster membership (which may
     * change dynamically), this list need not contain the full set of servers (you may want more than one,
     * though, in case a server is down).
     */
    public ConsumerBuilder bootstrapServers(String bootstrapServers) {
        return config(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * Automatically check the CRC32 of the records consumed. This ensures no on-the-wire or on-disk
     * corruption to the messages occurred. This check adds some overhead, so it may be disabled in cases
     * seeking extreme performance.
     */
    public ConsumerBuilder checkCrcs(boolean checkCrcs) {
        return config(ConsumerConfig.CHECK_CRCS_CONFIG, checkCrcs);
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
    public ConsumerBuilder clientDnsLookup(ClientDnsLookup clientDnsLookup) {
        return config(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, clientDnsLookup);
    }

    /**
     * An id string to pass to the server when making requests. The purpose of this is to be able to track
     * the source of requests beyond just ip/port by allowing a logical application name to be included in
     * server-side request logging.
     */
    public ConsumerBuilder clientId(String clientId) {
        return config(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    }

    /**
     * A rack identifier for this client. This can be any string value which indicates where this client is
     * physically located. It corresponds with the broker config 'broker.rack'
     */
    public ConsumerBuilder clientRack(String clientRack) {
        return config(ConsumerConfig.CLIENT_RACK_CONFIG, clientRack);
    }

    /**
     * Close idle connections after the number of milliseconds specified by this config.
     */
    public ConsumerBuilder connectionsMaxIdleMs(long connectionsMaxIdleMs) {
        return config(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, connectionsMaxIdleMs);
    }

    /**
     * Specifies the timeout (in milliseconds) for client APIs. This configuration is used as the default
     * timeout for all client operations that do not specify a <code>timeout</code> parameter.
     */
    public ConsumerBuilder defaultApiTimeoutMs(int defaultApiTimeoutMs) {
        return config(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, defaultApiTimeoutMs);
    }

    /**
     * If true the consumer's offset will be periodically committed in the background.
     */
    public ConsumerBuilder enableAutoCommit(boolean enableAutoCommit) {
        return config(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
    }

    /**
     * Whether internal topics matching a subscribed pattern should be excluded from the subscription. It
     * is always possible to explicitly subscribe to an internal topic.
     */
    public ConsumerBuilder excludeInternalTopics(boolean excludeInternalTopics) {
        return config(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, excludeInternalTopics);
    }

    /**
     * The maximum amount of data the server should return for a fetch request. Records are fetched in
     * batches by the consumer, and if the first record batch in the first non-empty partition of the fetch
     * is larger than this value, the record batch will still be returned to ensure that the consumer can
     * make progress. As such, this is not a absolute maximum. The maximum record batch size accepted by
     * the broker is defined via <code>message.max.bytes</code> (broker config) or
     * <code>max.message.bytes</code> (topic config). Note that the consumer performs multiple fetches in
     * parallel.
     */
    public ConsumerBuilder fetchMaxBytes(int fetchMaxBytes) {
        return config(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
    }

    /**
     * The maximum amount of time the server will block before answering the fetch request if there isn't
     * sufficient data to immediately satisfy the requirement given by fetch.min.bytes.
     */
    public ConsumerBuilder fetchMaxWaitMs(int fetchMaxWaitMs) {
        return config(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
    }

    /**
     * The minimum amount of data the server should return for a fetch request. If insufficient data is
     * available the request will wait for that much data to accumulate before answering the request. The
     * default setting of 1 byte means that fetch requests are answered as soon as a single byte of data is
     * available or the fetch request times out waiting for data to arrive. Setting this to something
     * greater than 1 will cause the server to wait for larger amounts of data to accumulate which can
     * improve server throughput a bit at the cost of some additional latency.
     */
    public ConsumerBuilder fetchMinBytes(int fetchMinBytes) {
        return config(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
    }

    /**
     * A unique string that identifies the consumer group this consumer belongs to. This property is
     * required if the consumer uses either the group management functionality by using
     * <code>subscribe(topic)</code> or the Kafka-based offset management strategy.
     */
    public ConsumerBuilder groupId(String groupId) {
        return config(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    }

    /**
     * A unique identifier of the consumer instance provided by the end user. Only non-empty strings are
     * permitted. If set, the consumer is treated as a static member, which means that only one instance
     * with this ID is allowed in the consumer group at any time. This can be used in combination with a
     * larger session timeout to avoid group rebalances caused by transient unavailability (e.g. process
     * restarts). If not set, the consumer will join the group as a dynamic member, which is the
     * traditional behavior.
     */
    public ConsumerBuilder groupInstanceId(String groupInstanceId) {
        return config(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, groupInstanceId);
    }

    /**
     * The expected time between heartbeats to the consumer coordinator when using Kafka's group management
     * facilities. Heartbeats are used to ensure that the consumer's session stays active and to facilitate
     * rebalancing when new consumers join or leave the group. The value must be set lower than
     * <code>session.timeout.ms</code>, but typically should be set no higher than 1/3 of that value. It
     * can be adjusted even lower to control the expected time for normal rebalances.
     */
    public ConsumerBuilder heartbeatIntervalMs(int heartbeatIntervalMs) {
        return config(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
    }

    /**
     * A list of classes to use as interceptors. Implementing the
     * <code>org.apache.kafka.clients.consumer.ConsumerInterceptor</code> interface allows you to intercept
     * (and possibly mutate) records received by the consumer. By default, there are no interceptors.
     */
    public ConsumerBuilder interceptorClasses(String interceptorClasses) {
        return config(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorClasses);
    }

    public ConsumerBuilder internalLeaveGroupOnClose(boolean internalLeaveGroupOnClose) {
        return config("internal.leave.group.on.close", internalLeaveGroupOnClose);
    }

    public ConsumerBuilder internalThrowOnFetchStableOffsetUnsupported(boolean internalThrowOnFetchStableOffsetUnsupported) {
        return config("internal.throw.on.fetch.stable.offset.unsupported", internalThrowOnFetchStableOffsetUnsupported);
    }

    /**
     * Controls how to read messages written transactionally. If set to <code>read_committed</code>,
     * consumer.poll() will only return transactional messages which have been committed. If set to
     * <code>read_uncommitted</code> (the default), consumer.poll() will return all messages, even
     * transactional messages which have been aborted. Non-transactional messages will be returned
     * unconditionally in either mode. <p>Messages will always be returned in offset order. Hence, in
     * <code>read_committed</code> mode, consumer.poll() will only return messages up to the last stable
     * offset (LSO), which is the one less than the offset of the first open transaction. In particular any
     * messages appearing after messages belonging to ongoing transactions will be withheld until the
     * relevant transaction has been completed. As a result, <code>read_committed</code> consumers will not
     * be able to read up to the high watermark when there are in flight transactions.</p><p> Further, when
     * in <code>read_committed</code> the seekToEnd method will return the LSO
     */
    public ConsumerBuilder isolationLevel(IsolationLevel isolationLevel) {
        return config(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
    }

    /**
     * Deserializer class for key that implements the
     * <code>org.apache.kafka.common.serialization.Deserializer</code> interface.
     */
    public ConsumerBuilder keyDeserializer(Class<? extends Deserializer<?>> keyDeserializer) {
        return config(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
    }

    /**
     * The maximum amount of data per-partition the server will return. Records are fetched in batches by
     * the consumer. If the first record batch in the first non-empty partition of the fetch is larger than
     * this limit, the batch will still be returned to ensure that the consumer can make progress. The
     * maximum record batch size accepted by the broker is defined via <code>message.max.bytes</code>
     * (broker config) or <code>max.message.bytes</code> (topic config). See fetch.max.bytes for limiting
     * the consumer request size.
     */
    public ConsumerBuilder maxPartitionFetchBytes(int maxPartitionFetchBytes) {
        return config(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
    }

    /**
     * The maximum delay between invocations of poll() when using consumer group management. This places an
     * upper bound on the amount of time that the consumer can be idle before fetching more records. If
     * poll() is not called before expiration of this timeout, then the consumer is considered failed and
     * the group will rebalance in order to reassign the partitions to another member. For consumers using
     * a non-null <code>group.instance.id</code> which reach this timeout, partitions will not be
     * immediately reassigned. Instead, the consumer will stop sending heartbeats and partitions will be
     * reassigned after expiration of <code>session.timeout.ms</code>. This mirrors the behavior of a
     * static consumer which has shutdown.
     */
    public ConsumerBuilder maxPollIntervalMs(int maxPollIntervalMs) {
        return config(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);
    }

    /**
     * The maximum number of records returned in a single call to poll(). Note, that
     * <code>max.poll.records</code> does not impact the underlying fetching behavior. The consumer will
     * cache the records from each fetch request and returns them incrementally from each poll.
     */
    public ConsumerBuilder maxPollRecords(int maxPollRecords) {
        return config(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    }

    /**
     * The period of time in milliseconds after which we force a refresh of metadata even if we haven't
     * seen any partition leadership changes to proactively discover any new brokers or partitions.
     */
    public ConsumerBuilder metadataMaxAgeMs(long metadataMaxAgeMs) {
        return config(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metadataMaxAgeMs);
    }

    /**
     * A list of classes to use as metrics reporters. Implementing the
     * <code>org.apache.kafka.common.metrics.MetricsReporter</code> interface allows plugging in classes
     * that will be notified of new metric creation. The JmxReporter is always included to register JMX
     * statistics.
     */
    public ConsumerBuilder metricReporters(String metricReporters) {
        return config(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, metricReporters);
    }

    /**
     * The number of samples maintained to compute metrics.
     */
    public ConsumerBuilder metricsNumSamples(int metricsNumSamples) {
        return config(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, metricsNumSamples);
    }

    /**
     * The highest recording level for metrics.
     */
    public ConsumerBuilder metricsRecordingLevel(Sensor.RecordingLevel metricsRecordingLevel) {
        return config(ConsumerConfig.METRICS_RECORDING_LEVEL_CONFIG, metricsRecordingLevel);
    }

    /**
     * The window of time a metrics sample is computed over.
     */
    public ConsumerBuilder metricsSampleWindowMs(long metricsSampleWindowMs) {
        return config(ConsumerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, metricsSampleWindowMs);
    }

    /**
     * A list of class names or class types, ordered by preference, of supported partition assignment
     * strategies that the client will use to distribute partition ownership amongst consumer instances
     * when group management is used. Available options
     * are:<ul><li><code>org.apache.kafka.clients.consumer.RangeAssignor</code>: The default assignor,
     * which works on a per-topic
     * basis.</li><li><code>org.apache.kafka.clients.consumer.RoundRobinAssignor</code>: Assigns partitions
     * to consumers in a round-robin
     * fashion.</li><li><code>org.apache.kafka.clients.consumer.StickyAssignor</code>: Guarantees an
     * assignment that is maximally balanced while preserving as many existing partition assignments as
     * possible.</li><li><code>org.apache.kafka.clients.consumer.CooperativeStickyAssignor</code>: Follows
     * the same StickyAssignor logic, but allows for cooperative rebalancing.</li></ul><p>Implementing the
     * <code>org.apache.kafka.clients.consumer.ConsumerPartitionAssignor</code> interface allows you to
     * plug in a custom assignment strategy.
     */
    public ConsumerBuilder partitionAssignmentStrategy(String partitionAssignmentStrategy) {
        return config(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy);
    }

    /**
     * The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS
     * default will be used.
     */
    public ConsumerBuilder receiveBufferBytes(int receiveBufferBytes) {
        return config(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes);
    }

    /**
     * The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly
     * failed to connect. If provided, the backoff per host will increase exponentially for each
     * consecutive connection failure, up to this maximum. After calculating the backoff increase, 20%
     * random jitter is added to avoid connection storms.
     */
    public ConsumerBuilder reconnectBackoffMaxMs(long reconnectBackoffMaxMs) {
        return config(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, reconnectBackoffMaxMs);
    }

    /**
     * The base amount of time to wait before attempting to reconnect to a given host. This avoids
     * repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by
     * the client to a broker.
     */
    public ConsumerBuilder reconnectBackoffMs(long reconnectBackoffMs) {
        return config(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
    }

    /**
     * The configuration controls the maximum amount of time the client will wait for the response of a
     * request. If the response is not received before the timeout elapses the client will resend the
     * request if necessary or fail the request if retries are exhausted.
     */
    public ConsumerBuilder requestTimeoutMs(int requestTimeoutMs) {
        return config(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
    }

    /**
     * The amount of time to wait before attempting to retry a failed request to a given topic partition.
     * This avoids repeatedly sending requests in a tight loop under some failure scenarios.
     */
    public ConsumerBuilder retryBackoffMs(long retryBackoffMs) {
        return config(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
    }

    /**
     * The fully qualified name of a SASL client callback handler class that implements the
     * AuthenticateCallbackHandler interface.
     */
    public ConsumerBuilder saslClientCallbackHandlerClass(Class<?> saslClientCallbackHandlerClass) {
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
    public ConsumerBuilder saslJaasConfig(String saslJaasConfig) {
        return config(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
    }

    /**
     * Kerberos kinit command path.
     */
    public ConsumerBuilder saslKerberosKinitCmd(String saslKerberosKinitCmd) {
        return config(SaslConfigs.SASL_KERBEROS_KINIT_CMD, saslKerberosKinitCmd);
    }

    /**
     * Login thread sleep time between refresh attempts.
     */
    public ConsumerBuilder saslKerberosMinTimeBeforeRelogin(long saslKerberosMinTimeBeforeRelogin) {
        return config(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN, saslKerberosMinTimeBeforeRelogin);
    }

    /**
     * The Kerberos principal name that Kafka runs as. This can be defined either in Kafka's JAAS config or
     * in Kafka's config.
     */
    public ConsumerBuilder saslKerberosServiceName(String saslKerberosServiceName) {
        return config(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, saslKerberosServiceName);
    }

    /**
     * Percentage of random jitter added to the renewal time.
     */
    public ConsumerBuilder saslKerberosTicketRenewJitter(double saslKerberosTicketRenewJitter) {
        return config(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER, saslKerberosTicketRenewJitter);
    }

    /**
     * Login thread will sleep until the specified window factor of time from last refresh to ticket's
     * expiry has been reached, at which time it will try to renew the ticket.
     */
    public ConsumerBuilder saslKerberosTicketRenewWindowFactor(double saslKerberosTicketRenewWindowFactor) {
        return config(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR, saslKerberosTicketRenewWindowFactor);
    }

    /**
     * The fully qualified name of a SASL login callback handler class that implements the
     * AuthenticateCallbackHandler interface. For brokers, login callback handler config must be prefixed
     * with listener prefix and SASL mechanism name in lower-case. For example,
     * listener.name.sasl_ssl.scram-sha-256.sasl.login.callback.handler.class=com.example.CustomScramLoginCallbackHandler
     */
    public ConsumerBuilder saslLoginCallbackHandlerClass(Class<?> saslLoginCallbackHandlerClass) {
        return config(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, saslLoginCallbackHandlerClass);
    }

    /**
     * The fully qualified name of a class that implements the Login interface. For brokers, login config
     * must be prefixed with listener prefix and SASL mechanism name in lower-case. For example,
     * listener.name.sasl_ssl.scram-sha-256.sasl.login.class=com.example.CustomScramLogin
     */
    public ConsumerBuilder saslLoginClass(Class<?> saslLoginClass) {
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
    public ConsumerBuilder saslLoginRefreshBufferSeconds(short saslLoginRefreshBufferSeconds) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS, saslLoginRefreshBufferSeconds);
    }

    /**
     * The desired minimum time for the login refresh thread to wait before refreshing a credential, in
     * seconds. Legal values are between 0 and 900 (15 minutes); a default value of 60 (1 minute) is used
     * if no value is specified.  This value and  sasl.login.refresh.buffer.seconds are both ignored if
     * their sum exceeds the remaining lifetime of a credential. Currently applies only to OAUTHBEARER.
     */
    public ConsumerBuilder saslLoginRefreshMinPeriodSeconds(short saslLoginRefreshMinPeriodSeconds) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS, saslLoginRefreshMinPeriodSeconds);
    }

    /**
     * Login refresh thread will sleep until the specified window factor relative to the credential's
     * lifetime has been reached, at which time it will try to refresh the credential. Legal values are
     * between 0.5 (50%) and 1.0 (100%) inclusive; a default value of 0.8 (80%) is used if no value is
     * specified. Currently applies only to OAUTHBEARER.
     */
    public ConsumerBuilder saslLoginRefreshWindowFactor(double saslLoginRefreshWindowFactor) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR, saslLoginRefreshWindowFactor);
    }

    /**
     * The maximum amount of random jitter relative to the credential's lifetime that is added to the login
     * refresh thread's sleep time. Legal values are between 0 and 0.25 (25%) inclusive; a default value of
     * 0.05 (5%) is used if no value is specified. Currently applies only to OAUTHBEARER.
     */
    public ConsumerBuilder saslLoginRefreshWindowJitter(double saslLoginRefreshWindowJitter) {
        return config(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER, saslLoginRefreshWindowJitter);
    }

    /**
     * SASL mechanism used for client connections. This may be any mechanism for which a security provider
     * is available. GSSAPI is the default mechanism.
     */
    public ConsumerBuilder saslMechanism(String saslMechanism) {
        return config(SaslConfigs.SASL_MECHANISM, saslMechanism);
    }

    /**
     * Protocol used to communicate with brokers. Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT,
     * SASL_SSL.
     */
    public ConsumerBuilder securityProtocol(String securityProtocol) {
        return config(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    }

    /**
     * A list of configurable creator classes each returning a provider implementing security algorithms.
     * These classes should implement the
     * <code>org.apache.kafka.common.security.auth.SecurityProviderCreator</code> interface.
     */
    public ConsumerBuilder securityProviders(String securityProviders) {
        return config(ConsumerConfig.SECURITY_PROVIDERS_CONFIG, securityProviders);
    }

    /**
     * The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS
     * default will be used.
     */
    public ConsumerBuilder sendBufferBytes(int sendBufferBytes) {
        return config(ConsumerConfig.SEND_BUFFER_CONFIG, sendBufferBytes);
    }

    /**
     * The timeout used to detect client failures when using Kafka's group management facility. The client
     * sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by
     * the broker before the expiration of this session timeout, then the broker will remove this client
     * from the group and initiate a rebalance. Note that the value must be in the allowable range as
     * configured in the broker configuration by <code>group.min.session.timeout.ms</code> and
     * <code>group.max.session.timeout.ms</code>.
     */
    public ConsumerBuilder sessionTimeoutMs(int sessionTimeoutMs) {
        return config(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
    }

    /**
     * The maximum amount of time the client will wait for the socket connection to be established. The
     * connection setup timeout will increase exponentially for each consecutive connection failure up to
     * this maximum. To avoid connection storms, a randomization factor of 0.2 will be applied to the
     * timeout resulting in a random range between 20% below and 20% above the computed value.
     */
    public ConsumerBuilder socketConnectionSetupTimeoutMaxMs(long socketConnectionSetupTimeoutMaxMs) {
        return config(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, socketConnectionSetupTimeoutMaxMs);
    }

    /**
     * The amount of time the client will wait for the socket connection to be established. If the
     * connection is not built before the timeout elapses, clients will close the socket channel.
     */
    public ConsumerBuilder socketConnectionSetupTimeoutMs(long socketConnectionSetupTimeoutMs) {
        return config(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, socketConnectionSetupTimeoutMs);
    }

    /**
     * A list of cipher suites. This is a named combination of authentication, encryption, MAC and key
     * exchange algorithm used to negotiate the security settings for a network connection using TLS or SSL
     * network protocol. By default all the available cipher suites are supported.
     */
    public ConsumerBuilder sslCipherSuites(String sslCipherSuites) {
        return config(SslConfigs.SSL_CIPHER_SUITES_CONFIG, sslCipherSuites);
    }

    /**
     * The list of protocols enabled for SSL connections. The default is 'TLSv1.2,TLSv1.3' when running
     * with Java 11 or newer, 'TLSv1.2' otherwise. With the default value for Java 11, clients and servers
     * will prefer TLSv1.3 if both support it and fallback to TLSv1.2 otherwise (assuming both support at
     * least TLSv1.2). This default should be fine for most cases. Also see the config documentation for
     * `ssl.protocol`.
     */
    public ConsumerBuilder sslEnabledProtocols(String sslEnabledProtocols) {
        return config(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, sslEnabledProtocols);
    }

    /**
     * The endpoint identification algorithm to validate server hostname using server certificate.
     */
    public ConsumerBuilder sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        return config(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);
    }

    /**
     * The class of type org.apache.kafka.common.security.auth.SslEngineFactory to provide SSLEngine
     * objects. Default value is org.apache.kafka.common.security.ssl.DefaultSslEngineFactory
     */
    public ConsumerBuilder sslEngineFactoryClass(Class<?> sslEngineFactoryClass) {
        return config(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, sslEngineFactoryClass);
    }

    /**
     * The password of the private key in the key store file orthe PEM key specified in `ssl.keystore.key'.
     * This is required for clients only if two-way authentication is configured.
     */
    public ConsumerBuilder sslKeyPassword(String sslKeyPassword) {
        return config(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
    }

    /**
     * The algorithm used by key manager factory for SSL connections. Default value is the key manager
     * factory algorithm configured for the Java Virtual Machine.
     */
    public ConsumerBuilder sslKeymanagerAlgorithm(String sslKeymanagerAlgorithm) {
        return config(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, sslKeymanagerAlgorithm);
    }

    /**
     * Certificate chain in the format specified by 'ssl.keystore.type'. Default SSL engine factory
     * supports only PEM format with a list of X.509 certificates
     */
    public ConsumerBuilder sslKeystoreCertificateChain(String sslKeystoreCertificateChain) {
        return config(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, sslKeystoreCertificateChain);
    }

    /**
     * Private key in the format specified by 'ssl.keystore.type'. Default SSL engine factory supports only
     * PEM format with PKCS#8 keys. If the key is encrypted, key password must be specified using
     * 'ssl.key.password'
     */
    public ConsumerBuilder sslKeystoreKey(String sslKeystoreKey) {
        return config(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, sslKeystoreKey);
    }

    /**
     * The location of the key store file. This is optional for client and can be used for two-way
     * authentication for client.
     */
    public ConsumerBuilder sslKeystoreLocation(String sslKeystoreLocation) {
        return config(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
    }

    /**
     * The store password for the key store file. This is optional for client and only needed if
     * 'ssl.keystore.location' is configured.  Key store password is not supported for PEM format.
     */
    public ConsumerBuilder sslKeystorePassword(String sslKeystorePassword) {
        return config(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
    }

    /**
     * The file format of the key store file. This is optional for client.
     */
    public ConsumerBuilder sslKeystoreType(String sslKeystoreType) {
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
    public ConsumerBuilder sslProtocol(String sslProtocol) {
        return config(SslConfigs.SSL_PROTOCOL_CONFIG, sslProtocol);
    }

    /**
     * The name of the security provider used for SSL connections. Default value is the default security
     * provider of the JVM.
     */
    public ConsumerBuilder sslProvider(String sslProvider) {
        return config(SslConfigs.SSL_PROVIDER_CONFIG, sslProvider);
    }

    /**
     * The SecureRandom PRNG implementation to use for SSL cryptography operations.
     */
    public ConsumerBuilder sslSecureRandomImplementation(String sslSecureRandomImplementation) {
        return config(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, sslSecureRandomImplementation);
    }

    /**
     * The algorithm used by trust manager factory for SSL connections. Default value is the trust manager
     * factory algorithm configured for the Java Virtual Machine.
     */
    public ConsumerBuilder sslTrustmanagerAlgorithm(String sslTrustmanagerAlgorithm) {
        return config(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, sslTrustmanagerAlgorithm);
    }

    /**
     * Trusted certificates in the format specified by 'ssl.truststore.type'. Default SSL engine factory
     * supports only PEM format with X.509 certificates.
     */
    public ConsumerBuilder sslTruststoreCertificates(String sslTruststoreCertificates) {
        return config(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, sslTruststoreCertificates);
    }

    /**
     * The location of the trust store file.
     */
    public ConsumerBuilder sslTruststoreLocation(String sslTruststoreLocation) {
        return config(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
    }

    /**
     * The password for the trust store file. If a password is not set, trust store file configured will
     * still be used, but integrity checking is disabled. Trust store password is not supported for PEM
     * format.
     */
    public ConsumerBuilder sslTruststorePassword(String sslTruststorePassword) {
        return config(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
    }

    /**
     * The file format of the trust store file.
     */
    public ConsumerBuilder sslTruststoreType(String sslTruststoreType) {
        return config(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, sslTruststoreType);
    }

    /**
     * Deserializer class for value that implements the
     * <code>org.apache.kafka.common.serialization.Deserializer</code> interface.
     */
    public ConsumerBuilder valueDeserializer(Class<? extends Deserializer<?>> valueDeserializer) {
        return config(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
    }

    // ### AUTOGENERATED BUILDER METHODS END ###

    public <T, S, U> U execute(ConsumerCallback<T, S, U> callback) {
        try (final Consumer<T, S> consumer = this.build()) {
            return callback.execute(consumer);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    public <KeyT, ValueT> void execute(ConsumerCallbackVoid<KeyT, ValueT> callback) {
        this.execute((ConsumerCallback<KeyT, ValueT, Void>) consumer -> {
            callback.execute(consumer);
            return null;
        });
    }

    private <T, S> Consumer<T, S> build() {
        return new KafkaConsumer<>(this.properties);
    }

    public ConsumerBuilder config(String configName, Object value) {
        this.properties.put(configName, value);
        return this;
    }

    public enum AutoOffsetReset implements IdentifiableEnum<String> {
        LATEST("latest"),
        EARLIEST("earliest"),
        NONE("none");

        private final String id;

        AutoOffsetReset(String id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id;
        }
    }

}
