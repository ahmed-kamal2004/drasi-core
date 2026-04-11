use crate::{config::MqttSourceConfig, pattern::PatternMatcher};
use std::{os::unix::process, sync::Arc};
use drasi_core::evaluation::variable_value::de;
use drasi_lib::channels::SourceChangeEvent;
use log::{trace, error, debug, info};
use tokio::task::JoinHandle;
use tokio::sync::mpsc;
use crate::utils::MqttPacket;
use crate::schema::{MqttSourceChange, convert_mqtt_to_source_change_event};
use crate::mqtt::MqttAppState;
use crate::adaptive_batcher::{AdaptiveBatcher, AdaptiveBatchConfig};
use drasi_lib::channels::{SourceEventWrapper, SourceEvent};
use drasi_lib::SourceBase;

#[derive(Debug)]
pub struct MqttProcessor {
    mapper: Arc<PatternMatcher>,
    processing_loop_handle: Option<JoinHandle<()>>,
    adaptive: bool,
}

impl MqttProcessor {

    //...... public methods

    pub fn new(source_id: impl Into<String>, config: &MqttSourceConfig) -> Self {
        let mapper: Arc<PatternMatcher> = Arc::new(PatternMatcher::new(&config.topic_mappings));

        let mut processor = Self {
            mapper,
            processing_loop_handle: None,
            adaptive: config.adaptive_enabled.unwrap_or(false),
        };

        processor
    }

    pub fn start_processing_loop(& mut self,source_id: String, mut rx: mpsc::Receiver<MqttPacket>, batch_tx: mpsc::Sender<SourceChangeEvent>) {
        let pattern_matcher = Arc::clone(&self.mapper);
        self.processing_loop_handle = Some(tokio::spawn(async move {
            Self::run_processing_loop(source_id.to_string(), pattern_matcher, rx, batch_tx).await;
        }));
    }

    pub fn start_adaptive_batcher_loop(&mut self, source_id: String, batch_rx: mpsc::Receiver<SourceChangeEvent>, dispatchers: Arc<tokio::sync::RwLock<Vec<Box<dyn drasi_lib::channels::ChangeDispatcher<SourceEventWrapper> + Send + Sync>>>>, adaptive_config: AdaptiveBatchConfig) {
        if self.adaptive {
            self.processing_loop_handle = Some(tokio::spawn(async move {
                Self::run_adaptive_batcher_loop(batch_rx, dispatchers, adaptive_config, source_id).await;
            }));
        }
    }


    //...... internal processing methods

    async fn run_adaptive_batcher_loop(
        batch_rx: mpsc::Receiver<SourceChangeEvent>,
        dispatchers: Arc<tokio::sync::RwLock<Vec<Box<dyn drasi_lib::channels::ChangeDispatcher<SourceEventWrapper> + Send + Sync>>>>,
        adaptive_config: AdaptiveBatchConfig,
        source_id: String,
    ) {
        let mut batcher = AdaptiveBatcher::new(batch_rx, adaptive_config.clone());
        let mut total_events = 0u64;
        let mut total_batches = 0u64;

        info!(
            "[{}] MQTT Batcher with config: {:?}",
            source_id, adaptive_config
        );

        while let Some(batch) = batcher.next_batch().await {
            if batch.is_empty() {
                debug!(
                    "[{}] MQTT Batcher received empty batch, skipping",
                    source_id
                );
                continue;
            }

            println!(
                "[{}] MQTT Batcher received batch of {} events, processing...",
                source_id,
                batch.len()
            );

            let batch_size = batch.len();

            total_events += batch_size as u64;
            total_batches += 1;

            debug!(
                "[{source_id}] MQTT Batcher forwarding batch #{total_batches} with {batch_size} events to dispatchers"
            );

            let mut sent_count = 0;
            let mut failed_count = 0;

            for (idx, event) in batch.into_iter().enumerate() {
                debug!(
                    "[{}] Batch #{}, dispatching event {}/{}",
                    source_id,
                    total_batches,
                    idx + 1,
                    batch_size
                );

                let mut profiling = drasi_lib::profiling::ProfilingMetadata::new();
                profiling.source_send_ns = Some(drasi_lib::profiling::timestamp_ns());

                let wrapper = SourceEventWrapper::with_profiling(
                    event.source_id.clone(),
                    SourceEvent::Change(event.change),
                    event.timestamp,
                    profiling,
                );

                if let Err(e) =
                    SourceBase::dispatch_from_task(dispatchers.clone(), wrapper.clone(), &source_id)
                        .await
                {
                    error!(
                        "[{}] Batch #{}, failed to dispatch event {}/{} (no subscribers): {}",
                        source_id,
                        total_batches,
                        idx + 1,
                        batch_size,
                        e
                    );
                    failed_count += 1;
                } else {
                    debug!(
                        "[{}] Batch #{}, successfully dispatched event {}/{}",
                        source_id,
                        total_batches,
                        idx + 1,
                        batch_size
                    );
                    sent_count += 1;
                }
            }

            debug!(
                "[{source_id}] Batch #{total_batches} complete: {sent_count} dispatched, {failed_count} failed"
            );

            if total_batches.is_multiple_of(100) {
                info!(
                    "[{}] Adaptive MQTT metrics - Batches: {}, Events: {}, Avg batch size: {:.1}",
                    source_id,
                    total_batches,
                    total_events,
                    total_events as f64 / total_batches as f64
                );
            }
        }

        info!(
            "[{source_id}] Adaptive MQTT batcher stopped - Total batches: {total_batches}, Total events: {total_events}"
        );
    }

    async fn run_processing_loop(source_id: String,matcher: Arc<PatternMatcher>, mut rx: mpsc::Receiver<MqttPacket>, batch_tx: mpsc::Sender<SourceChangeEvent>) {
        while let Some(packet) = rx.recv().await {
            println!("Received message - Topic: {}, Payload: {:?}, Timestamp: {}", packet.topic, packet.payload, packet.timestamp);

            // generate source changes from the packet and topic name
            let source_changes = Self::process(&matcher, &packet);

            // send to the batcher
            Self::send_to_batcher(&source_id.clone(), &batch_tx, source_changes).await;
        }
    }

    fn process(mapper: &PatternMatcher, packet: &MqttPacket) -> Vec<MqttSourceChange>{

        if let Ok(matched) = mapper.router.at(&packet.topic) {
            // 1. Collect params into a readable format
            let params_debug: String = matched
                .params
                .iter()
                .map(|(key, val)| format!("{}={}", key, val))
                .collect::<Vec<_>>()
                .join(", ");

            // 2. Log everything using info!
            info!(
                "Route Matched! Value: {:?},\n Params: [{}],\n Path: {}",
                matched.value, 
                params_debug, 
                packet.topic
            );
        } else {
            info!("No match found for path: {}", packet.topic);
        }

        vec![]
    }

    async fn send_to_batcher(source_id: &str, batch_tx: &mpsc::Sender<SourceChangeEvent>, source_changes: Vec<MqttSourceChange>) {
        let source_id = source_id.to_string();
        for (idx, change) in source_changes.into_iter().enumerate() {
            match convert_mqtt_to_source_change_event(&change, &source_id) {
                Ok(source_change) => {
                    let change_event = SourceChangeEvent {
                        source_id: source_id.to_string(),
                        change: source_change,
                        timestamp: chrono::Utc::now(),
                    };

                    if let Err(e) = batch_tx.send(change_event).await {
                        error!(
                            "[{}] Failed to send change event to batcher for change {}: {}",
                            source_id, idx, e
                        );
                    } else {
                        debug!(
                            "[{}] Successfully sent change event to batcher for change {}",
                            source_id, idx
                        );
                    } 

                }
                Err(e) => {
                    error!(
                        "[{}] Failed to convert MQTT change to source change for change {}: {}",
                        source_id, idx, e
                    );
                }
            }
        }

    }
}