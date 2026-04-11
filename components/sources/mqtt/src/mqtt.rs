use crate::adaptive_batcher::{AdaptiveBatchConfig, AdaptiveBatcher};
use crate::config::MqttSourceConfig;
use crate::connection::MqttConnection;
use crate::schema::MqttSourceChange;
use drasi_core::evaluation::functions::async_trait;
use drasi_core::models::SourceChange;
use drasi_lib::config::SourceSubscriptionSettings;
use drasi_lib::queries::base;
use drasi_lib::sources::base::{SourceBase, SourceBaseParams};
use drasi_lib::ComponentStatus;
use drasi_lib::Source;
use std::collections::HashMap;
use std::os::unix::process;
use crate::processor::MqttProcessor;

use anyhow::Result;
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

use drasi_lib::channels::{ComponentType, *};
use drasi_lib::SourceRuntimeContext;
use tracing::Instrument;

/// MQTT source with configurable adaptive batching.
///
/// This source acts as a subscriber for receiving data change events.
/// It supports both receiving single-event and multiple batched events, with adaptive
/// batching for optimized throughput.
///
/// # Fields
///
/// - `base`: Common source functionality (dispatchers, status, lifecycle)
/// - `config`: MQTT-specific configuration (host, port, timeout)
/// - `adaptive_config`: Adaptive batching settings for throughput optimization
pub struct MQTTSource {
    /// Base source implementation providing common functionality
    base: SourceBase,
    /// MQTT source configuration
    config: MqttSourceConfig,
    /// Adaptive batching configuration for throughput optimization
    adaptive_config: AdaptiveBatchConfig,
}

/// Batch event request that can accept multiple events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchEventRequest {
    pub events: Vec<MqttSourceChange>,
}

/// MQTT source app state with batching channel.
///
/// Shared state passed to the MQTTConnectionWrapper::process_events.
#[derive(Clone)]
pub struct MqttAppState {
    /// The source ID for validation against incoming requests
    pub source_id: String,
    /// Channel for sending events to the adaptive batcher
    pub batch_tx: mpsc::Sender<SourceChangeEvent>,
}

impl MQTTSource {
    /// Create a new MQTT source.
    ///
    /// The event channel is automatically injected when the source is added
    /// to DrasiLib via `add_source()`.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this source instance
    /// * `config` - MQTT source configuration
    ///
    /// # Returns
    ///
    /// A new `MQTTSource` instance, or an error if construction fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the base source cannot be initialized.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use drasi_source_mqtt::{MQTTSource, MQTTSourceBuilder};
    ///
    /// let config = MQTTSourceBuilder::new()
    ///     .with_host("0.0.0.0")
    ///     .with_port(9001)
    ///     .with_topic("my/mqtt/topic")
    ///     .with_qos(QualityOfService::AtLeastOnce)
    ///     .build();
    ///
    /// let source = MQTTSource::new("my-mqtt-source", config)?;
    /// ```
    pub fn new(id: impl Into<String>, config: MqttSourceConfig) -> Result<Self> {
        let id = id.into();
        let params = SourceBaseParams::new(id);

        let mut adaptive_config = AdaptiveBatchConfig::default();

        // Allow overriding adaptive parameters from config
        if let Some(max_batch) = config.adaptive_max_batch_size {
            adaptive_config.max_batch_size = max_batch;
        }
        if let Some(min_batch) = config.adaptive_min_batch_size {
            adaptive_config.min_batch_size = min_batch;
        }
        if let Some(max_wait_ms) = config.adaptive_max_wait_ms {
            adaptive_config.max_wait_time = Duration::from_millis(max_wait_ms);
        }
        if let Some(min_wait_ms) = config.adaptive_min_wait_ms {
            adaptive_config.min_wait_time = Duration::from_millis(min_wait_ms);
        }
        if let Some(window_secs) = config.adaptive_window_secs {
            adaptive_config.throughput_window = Duration::from_secs(window_secs);
        }
        if let Some(enabled) = config.adaptive_enabled {
            adaptive_config.adaptive_enabled = enabled;
        }
        Ok(Self {
            base: SourceBase::new(params)?,
            config,
            adaptive_config,
        })
    }

    /// Create a new MQTT source with custom dispatch settings.
    ///
    /// The event channel is automatically injected when the source is added
    /// to DrasiLib via `add_source()`.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this source instance
    /// * `config` - MQTT source configuration
    /// * `dispatch_mode` - Optional dispatch mode (Channel, Direct, etc.)
    /// * `dispatch_buffer_capacity` - Optional buffer capacity for channel dispatch
    ///
    /// # Returns
    ///
    /// A new `MQTTSource` instance with custom dispatch settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the base source cannot be initialized.
    pub fn with_dispatch(
        id: impl Into<String>,
        config: MqttSourceConfig,
        dispatch_mode: Option<DispatchMode>,
        dispatch_buffer_capacity: Option<usize>,
    ) -> Result<Self> {
        let id = id.into();
        let mut params = SourceBaseParams::new(id);

        if let Some(mode) = dispatch_mode {
            params = params.with_dispatch_mode(mode);
        }
        if let Some(capacity) = dispatch_buffer_capacity {
            params = params.with_dispatch_buffer_capacity(capacity);
        }

        let mut adaptive_config = AdaptiveBatchConfig::default();

        if let Some(max_batch) = config.adaptive_max_batch_size {
            adaptive_config.max_batch_size = max_batch;
        }
        if let Some(min_batch) = config.adaptive_min_batch_size {
            adaptive_config.min_batch_size = min_batch;
        }
        if let Some(max_wait_ms) = config.adaptive_max_wait_ms {
            adaptive_config.max_wait_time = Duration::from_millis(max_wait_ms);
        }
        if let Some(min_wait_ms) = config.adaptive_min_wait_ms {
            adaptive_config.min_wait_time = Duration::from_millis(min_wait_ms);
        }
        if let Some(window_secs) = config.adaptive_window_secs {
            adaptive_config.throughput_window = Duration::from_secs(window_secs);
        }
        if let Some(enabled) = config.adaptive_enabled {
            adaptive_config.adaptive_enabled = enabled;
        }

        Ok(Self {
            base: SourceBase::new(params)?,
            config,
            adaptive_config,
        })
    }

    pub(crate) fn from_parts(
        base: SourceBase,
        config: MqttSourceConfig,
        adaptive_config: AdaptiveBatchConfig,
    ) -> Self {
        Self {
            base,
            config,
            adaptive_config,
        }
    }

    pub fn config(&self) -> &MqttSourceConfig {
        &self.config
    }

    pub fn adaptive_config(&self) -> &AdaptiveBatchConfig {
        &self.adaptive_config
    }

}

#[async_trait]
impl Source for MQTTSource {
    fn id(&self) -> &str {
        &self.base.id
    }

    fn type_name(&self) -> &str {
        "mqtt"
    }

    fn properties(&self) -> std::collections::HashMap<String, serde_json::Value> {
        let mut props = HashMap::new();

        let topics = self
            .config
            .topics
            .iter()
            .map(|topic| serde_json::Value::String(topic.topic.clone()))
            .collect();

        props.insert(
            "broker_addr".to_string(),
            serde_json::Value::String(self.config.broker_addr.clone()),
        );
        props.insert(
            "port".to_string(),
            serde_json::Value::Number(self.config.port.into()),
        );
        props.insert("topics".to_string(), serde_json::Value::Array(topics));

        props
    }

    fn auto_start(&self) -> bool {
        self.base.get_auto_start()
    }




    async fn start(&self) -> Result<()> {
        info!("[{}] Starting MQTT source", self.id());

        // set the source status
        self.base.set_status(ComponentStatus::Starting).await;
        self.base
            .send_component_event(
                ComponentStatus::Starting,
                Some("Starting MQTT Source".to_string()),
            )
            .await?;



        let source_id = self.base.id.clone();
        let instance_id = self
            .base
            .context()
            .await
            .map(|c| c.instance_id)
            .unwrap_or_default();


        // Start the subscriber
        let (error_tx, error_rx) = tokio::sync::oneshot::channel();

        let span = tracing::info_span!(
            "mqtt_source_server",
            instance_id = %instance_id,
            component_id = %source_id.clone(),
            component_type = "source"
        );

        let config = self.config.clone();
        let source_id_for_task = self.id().to_string();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let (processer_tx, processor_rx) = mpsc::channel(100);

        // create batch channel.
        let batch_channel_capacity = self.adaptive_config.recommended_channel_capacity();
        let (batch_tx, batch_rx) = mpsc::channel::<SourceChangeEvent>(batch_channel_capacity);


        // start processor task
        let mut processor = MqttProcessor::new(source_id.clone(), &config);
        processor.start_processing_loop(source_id.clone(), processor_rx, batch_tx);


        // start adaptive batcher task
        let dispatchers = self.base.dispatchers.clone();
        let adaptive_config = self.adaptive_config.clone();
        let batcher_source_id = source_id.clone();
        processor.start_adaptive_batcher_loop(batcher_source_id, batch_rx, dispatchers, adaptive_config);

        // start mqtt connection task
        let server_handle = tokio::spawn(
            async move {
                let (connection_shutdown_tx, connection_shutdown_rx) =
                    tokio::sync::oneshot::channel();

                if let Err(error) =
                    MqttConnection::new(source_id_for_task, &config, connection_shutdown_rx, processer_tx).await
                {
                    let _ = error_tx.send(error.to_string());
                    return;
                }

                let _ = shutdown_rx.await;
                let _ = connection_shutdown_tx.send(());
            }
            .instrument(span),
        );

        *self.base.task_handle.write().await = Some(server_handle);
        *self.base.shutdown_tx.write().await = Some(shutdown_tx);

        match timeout(Duration::from_millis(500), error_rx).await {
            Ok(Ok(error_msg)) => {
                self.base.set_status(ComponentStatus::Error).await;
                return Err(anyhow::anyhow!("{error_msg}"));
            }
            _ => {
                self.base.set_status(ComponentStatus::Running).await;
            }
        }

        // end, set status to running.
        self.base
            .send_component_event(
                ComponentStatus::Running,
                Some(format!("MQTT source running on {}:{}", self.config.broker_addr, self.config.port)),
            )
            .await?;

        Ok(())
    }


    async fn stop(&self) -> Result<()> {
        info!("[{}] Stopping MQTT source", self.base.id);

        self.base.set_status(ComponentStatus::Stopping).await;
        self.base
            .send_component_event(
                ComponentStatus::Stopping,
                Some("Stopping MQTT source".to_string()),
            )
            .await?;

        if let Some(tx) = self.base.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        }

        if let Some(handle) = self.base.task_handle.write().await.take() {
            let _ = timeout(Duration::from_secs(5), handle).await;
        }

        self.base.set_status(ComponentStatus::Stopped).await;
        self.base
            .send_component_event(
                ComponentStatus::Stopped,
                Some("MQTT source stopped".to_string()),
            )
            .await?;

        Ok(())
    }

    async fn status(&self) -> ComponentStatus {
        self.base.get_status().await
    }

    async fn subscribe(
        &self,
        settings: drasi_lib::config::SourceSubscriptionSettings,
    ) -> Result<SubscriptionResponse> {
        self.base.subscribe_with_bootstrap(&settings, "MQTT").await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn deprovision(&self) -> Result<()> {
        Ok(())
    }

    async fn initialize(&self, context: SourceRuntimeContext) {
        self.base.initialize(context).await;
    }
}
