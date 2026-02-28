use drasi_source_mqtt::MQTTSource;
use anyhow::Result;
use drasi_reaction_log::{QueryConfig, TemplateSpec};
use drasi_lib::Query;
use drasi_reaction_log::LogReaction;
use drasi_lib::DrasiLib;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info"),
    ).init();

    println!("╔════════════════════════════════════════════╗");
    println!("║     DrasiLib Temperature Monitor Example   ║");
    println!("╚════════════════════════════════════════════╝\n");


    // TODO: Add bootstrapper


    // Create MQTT source

    let mqtt_source = MQTTSource::builder("mqtt-source")
        .with_host("localhost")
        .with_port(1883)
        .with_topic("sensors/temperature")
        .with_qos(drasi_source_mqtt::model::QualityOfService::AtLeastOnce)
        .build()?;

    let all_readings_query = Query::cypher("all-readings")
        .query(r#"
            MATCH (rd:readings)
            RETURN rd.symbol AS symbol,
                   rd.val AS value
        "#)
        .from_source("mqtt-source")
        .auto_start(true)
        .enable_bootstrap(true)
        .build();

    
    
    let default_template = QueryConfig {
         added: Some(TemplateSpec{
             template: "[{{query_name}}] + {{after.symbol}}: ${{after.val}}".to_string(),
             ..Default::default()
         }),
         updated: Some(TemplateSpec{
             template: "[{{query_name}}] ~ {{after.symbol}}: ${{before.val}} -> ${{after.val}}".to_string(),
             ..Default::default()
         }),
         deleted: Some(TemplateSpec{
             template: "[{{query_name}}] - {{before.symbol}} removed".to_string(),
            ..Default::default()
         }),
     };

    let log_reaction = LogReaction::builder("console-logger")
        .from_query("all-readings")
        .with_default_template(default_template)
        .build().unwrap();


    let core = Arc::new(
        DrasiLib::builder()
            .with_source(mqtt_source)
            .with_reaction(log_reaction)
            .with_query(all_readings_query)
            .build()
            .await?
    );

    core.start().await?;

    tokio::signal::ctrl_c().await?;
    core.stop().await?;

    Ok(())
}