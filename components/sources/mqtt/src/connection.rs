use core::panic;
use std::os::unix::process;
use std::{option, time::Duration};

use crate::config::{MqttQoS, MqttTransportMode};
use crate::{config::MqttSourceConfig, mqtt};
use drasi_core::evaluation::functions::async_trait;
use rumqttc::v5::{
    AsyncClient as AsyncClientV5, EventLoop as EventLoopV5, MqttOptions as MqttOptionsV5,
};
use rumqttc::{
    v5::{mqttbytes::v5::ConnectReturnCode, ConnectionError},
    AsyncClient, EventLoop, MqttOptions,
};
use tracing::event;

use crate::utils::MqttPacket;
use log::{debug, error, info, trace, warn};

macro_rules! run_event_loop {
    ($event_loop:expr, $shutdown_rx:expr, $processer_tx:expr) => {
        loop {
            tokio::select! {
                _ = &mut $shutdown_rx => {
                    info!("Shutdown signal received, stopping MQTT event loop");
                    break;
                },
                event = $event_loop.poll() => {
                    match event {
                        Ok(event) =>{
                            let packet = event.to_mqtt_packet();
                            if let Some(packet) = packet {
                                info!("Received MQTT packet - Topic: {}, Payload: {:?}, Timestamp: {}", packet.topic, packet.payload, packet.timestamp);
                                if let Err(e) = $processer_tx.send(packet).await {
                                        error!("Failed to send MQTT packet to processor: {:?}", e);
                                }
                            }
                        },
                        Err(e) => error!("MQTT event loop error: {:?}", e),
                    }
                }
            }
        }
    };
}

macro_rules! common_config_to_mqtt_options {
    ($options:expr, $config:expr) => {
        if let Some(tranport) = $config.transport.as_ref() {
            match tranport {
                crate::config::MqttTransportMode::TLS {
                    ca,
                    alpn,
                    client_auth,
                } => {
                    let tls_config = rumqttc::TlsConfiguration::Simple {
                        ca: ca.clone(),
                        alpn: alpn.clone(),
                        client_auth: client_auth.clone(),
                    };
                    $options.set_transport(rumqttc::Transport::Tls(tls_config));
                }
                _ => {}
            }
        }

        if let Some(request_channel_capacity) = $config.request_channel_capacity {
            $options.set_request_channel_capacity(request_channel_capacity);
        }

        if let Some(keep_alive) = $config.keep_alive {
            $options.set_keep_alive(Duration::from_secs(keep_alive));
        }
    };
}

trait ToMqttPacket {
    fn to_mqtt_packet(self) -> Option<MqttPacket>;
}

impl ToMqttPacket for rumqttc::Event {
    fn to_mqtt_packet(self) -> Option<MqttPacket> {
        if let rumqttc::Event::Incoming(rumqttc::Packet::Publish(p)) = self {
            Some(MqttPacket {
                topic: p.topic,
                payload: p.payload,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            })
        } else {
            None
        }
    }
}

impl ToMqttPacket for rumqttc::v5::Event {
    fn to_mqtt_packet(self) -> Option<MqttPacket> {
        if let rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::Publish(p)) = self {
            Some(MqttPacket {
                topic: str::from_utf8(&p.topic).unwrap_or_default().to_string(),
                payload: p.payload,
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            })
        } else {
            None
        }
    }
}

pub enum MqttAsyncClientWrapper {
    AsyncClientV3(AsyncClient),
    AsyncClientV5(AsyncClientV5),
}

enum MqttEventLoopWrapper {
    EventLoopV3 { event_loop: EventLoop },
    EventLoopV5 { event_loop: EventLoopV5 },
}

#[async_trait]
trait MqttEventLoop {
    async fn poll(&mut self) -> anyhow::Result<rumqttc::Event>;
}

pub(crate) struct MqttConnection {
    client: MqttAsyncClientWrapper,
    event_loop_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MqttConnection {
    //....... Public methods

    pub async fn new(
        id: impl Into<String>,
        config: &MqttSourceConfig,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        mut processer_tx: tokio::sync::mpsc::Sender<MqttPacket>,
    ) -> anyhow::Result<Self> {
        let id_v5 = id.into().clone();
        let id_v3 = id_v5.clone();
        // try Mqtt v5 first
        let options_v5 = Self::config_to_mqtt_options_v5(id_v5, config);
        let (client_v5, mut event_loop_v5) =
            AsyncClientV5::new(options_v5, config.event_channel_capacity);
        for trial in 0..3 {
            match event_loop_v5.poll().await {
                Ok(event) => {
                    info!("Successfully connected to MQTT broker using v5 options");
                    let mut connection = Self {
                        client: MqttAsyncClientWrapper::AsyncClientV5(client_v5),
                        event_loop_handle: None,
                    };
                    connection
                        .run_subscription_loop(
                            shutdown_rx,
                            MqttEventLoopWrapper::EventLoopV5 {
                                event_loop: event_loop_v5,
                            },
                            config,
                            processer_tx,
                        )
                        .await;
                    return Ok(connection);
                }
                Err(ConnectionError::ConnectionRefused(
                    ConnectReturnCode::UnsupportedProtocolVersion,
                )) => {
                    error!("Failed to connect using MQTT v5 options: Unsupported protocol version. Attempting MQTT v3 fallback...");
                    break;
                }
                Err(e) => {
                    error!(
                        "Failed to connect using MQTT v5 options on trial {}: {:?}",
                        trial + 1,
                        e
                    );
                }
            };
        }

        // fallback to Mqtt v3.1.1
        let options_v3 = Self::config_to_mqtt_options_v3(id_v3, config);
        let (client_v3, mut event_loop_v3) =
            AsyncClient::new(options_v3, config.event_channel_capacity);

        for trial in 0..3 {
            match event_loop_v3.poll().await {
                Ok(event) => {
                    info!("Successfully connected to MQTT broker using v3 options");
                    let mut connection = Self {
                        client: MqttAsyncClientWrapper::AsyncClientV3(client_v3),
                        event_loop_handle: None,
                    };
                    connection
                        .run_subscription_loop(
                            shutdown_rx,
                            MqttEventLoopWrapper::EventLoopV3 {
                                event_loop: event_loop_v3,
                            },
                            config,
                            processer_tx,
                        )
                        .await;
                    return Ok(connection);
                }
                Err(e) => {
                    error!(
                        "Failed to connect using MQTT v3 options on trial {}: {:?}",
                        trial + 1,
                        e
                    );
                }
            };
        }

        Err(anyhow::anyhow!("Failed to create MQTT client with both v5 and v3 options. Check configuration for errors."))
    }

    //....... Private helper methods

    async fn run_subscription_loop(
        &mut self,
        mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
        mut event_loop: MqttEventLoopWrapper,
        config: &MqttSourceConfig,
        mut processer_tx: tokio::sync::mpsc::Sender<MqttPacket>,
    ) {
        self.subscribe_to_topics(config).await.unwrap_or_else(|e| {
            error!("Failed to subscribe to topics: {:?}", e);
            return;
        });

        let mqtt_version: u8 = match &self.client {
            MqttAsyncClientWrapper::AsyncClientV5(_) => 5,
            MqttAsyncClientWrapper::AsyncClientV3(_) => 3,
        };

        self.event_loop_handle = Some(tokio::spawn(async move {
            if mqtt_version == 5 {
                let mut event_loop_v5 = match event_loop {
                    MqttEventLoopWrapper::EventLoopV5 { event_loop } => event_loop,
                    _ => {
                        error!("Expected MQTT v5 event loop");
                        return;
                    }
                };
                run_event_loop!(event_loop_v5, shutdown_rx, processer_tx);
            } else {
                let mut event_loop_v3 = match event_loop {
                    MqttEventLoopWrapper::EventLoopV3 { event_loop } => event_loop,
                    _ => {
                        error!("Expected MQTT v3 event loop");
                        return;
                    }
                };
                run_event_loop!(event_loop_v3, shutdown_rx, processer_tx);
            }
        }))
    }

    async fn subscribe_to_topics(&mut self, config: &MqttSourceConfig) -> anyhow::Result<()> {
        for topic_config in &config.topics {
            let topic = topic_config.topic.clone();
            match &mut self.client {
                MqttAsyncClientWrapper::AsyncClientV5(client_v5) => {
                    let qos = match topic_config.qos {
                        MqttQoS::ZERO => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
                        MqttQoS::ONE => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
                        MqttQoS::TWO => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
                    };
                    client_v5.subscribe(topic.clone(), qos).await.map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to subscribe to topic '{}' with MQTT v5 client: {:?}",
                            topic,
                            e
                        )
                    })?;
                }
                MqttAsyncClientWrapper::AsyncClientV3(client_v3) => {
                    let qos = match topic_config.qos {
                        MqttQoS::ZERO => rumqttc::QoS::AtMostOnce,
                        MqttQoS::ONE => rumqttc::QoS::AtLeastOnce,
                        MqttQoS::TWO => rumqttc::QoS::ExactlyOnce,
                    };
                    client_v3.subscribe(topic.clone(), qos).await.map_err(|e| {
                        anyhow::anyhow!(
                            "Failed to subscribe to topic '{}' with MQTT v3 client: {:?}",
                            topic,
                            e
                        )
                    })?;
                }
            }
        }
        Ok(())
    }

    fn config_to_mqtt_options_v5(
        id: impl Into<String>,
        config: &MqttSourceConfig,
    ) -> MqttOptionsV5 {
        let mut options = MqttOptionsV5::new(id, config.broker_addr.clone(), config.port);

        // Common between v5 and v3.1.1
        common_config_to_mqtt_options!(options, config);

        if let Some(max_inflight) = config.max_inflight {
            options.set_outgoing_inflight_upper_limit(max_inflight);
        }
        if let Some(clean_start) = config.clean_start {
            options.set_clean_start(clean_start);
        }

        // v5 specific options
        if let Some(conn_timeout) = config.conn_timeout {
            options.set_connection_timeout(conn_timeout);
        }
        if let Some(connect_properties) = config.connect_properties.as_ref() {
            let connection_props = connect_properties.to_connection_properties();
            options.set_connect_properties(connection_props);
        }

        options
    }

    fn config_to_mqtt_options_v3(id: impl Into<String>, config: &MqttSourceConfig) -> MqttOptions {
        let mut options = MqttOptions::new(id, config.broker_addr.clone(), config.port);

        // Common between v5 and v3.1.1
        common_config_to_mqtt_options!(options, config);

        if let Some(max_inflight) = config.max_inflight {
            options.set_inflight(max_inflight);
        }
        if let Some(clean_start) = config.clean_start {
            options.set_clean_session(clean_start);
        }

        // v3 specific options
        if let Some(max_incoming_packet_size) = config.max_incoming_packet_size {
            options.set_max_packet_size(max_incoming_packet_size, max_incoming_packet_size);
            // TODO
        }

        if let Some(max_outgoing_packet_size) = config.max_outgoing_packet_size {
            options.set_max_packet_size(max_outgoing_packet_size, max_outgoing_packet_size);
            // TODO
        }

        options
    }
}
