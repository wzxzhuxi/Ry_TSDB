mod db;
mod error;
mod gorilla;
mod sstable;
mod wal;
mod server;
mod types;

use std::sync::Arc;
use log::info;
use db::{DbConfig, SimpleTSDB};
use crate::server::TsdbServer;
use crate::types::DataPoint;

#[tokio::main]
async fn main() -> error::Result<()> {
    // 初始化日志系统
    env_logger::init();
    
    info!("启动支持标签和多字段数据的LSM-Tree TSDB（Gorilla压缩 + 零拷贝技术）");
    
    // 配置数据库
    let config = DbConfig {
        sstable_dir: "./data/sstable".to_string(),
        wal_path: "./data/wal.log".to_string(),
        memtable_size_threshold: 1000,
    };
    
    // 打开数据库
    let db = Arc::new(SimpleTSDB::open(config)?);
    
    // 写入示例数据
    write_sample_data(&db)?;
    
    // 创建并启动服务器
    let server = TsdbServer::new(db, "127.0.0.1:6364".to_string());
    info!("服务器将在 127.0.0.1:6364 监听请求");
    
    // 启动服务器
    server.run().await?;
    
    Ok(())
}

fn write_sample_data(db: &SimpleTSDB) -> error::Result<()> {
    info!("写入示例数据...");
    
    // 写入CPU监控数据
    for i in 0..50 {
        let ts = 1622000000 + i * 60;
        
        // CPU数据，带有标签和多个字段
        let mut cpu_point = DataPoint::new(ts);
        cpu_point.add_tag("host", "server1")
                 .add_tag("region", "us-west")
                 .add_field("usage", 25.0 + (i as f64 % 10.0))
                 .add_field("idle", 75.0 - (i as f64 % 10.0))
                 .add_field("system", 10.0 + (i as f64 * 0.1));
                 
        db.write_point("cpu", cpu_point)?;
        
        // 内存数据，不同标签组合
        let mut mem_point = DataPoint::new(ts);
        mem_point.add_tag("host", "server1")
                 .add_tag("region", "us-west")
                 .add_field("used", 8192.0 + (i as f64 * 10.0))
                 .add_field("free", 16384.0 - (i as f64 * 10.0))
                 .add_field("cached", 4096.0 + (i as f64 * 5.0));
                 
        db.write_point("memory", mem_point)?;
        
        // 不同主机的CPU数据
        let mut cpu_point2 = DataPoint::new(ts);
        cpu_point2.add_tag("host", "server2")
                  .add_tag("region", "eu-central")
                  .add_field("usage", 15.0 + (i as f64 % 8.0))
                  .add_field("idle", 85.0 - (i as f64 % 8.0))
                  .add_field("system", 8.0 + (i as f64 * 0.05));
                  
        db.write_point("cpu", cpu_point2)?;
    }
    
    // 批量写入网络数据
    let mut network_points = Vec::new();
    for i in 0..100 {
        let ts = 1622000000 + i * 30;
        
        let mut point = DataPoint::new(ts);
        point.add_tag("host", "server1")
             .add_tag("interface", "eth0")
             .add_field("tx_bytes", 1024.0 * (1.0 + i as f64 * 0.02))
             .add_field("rx_bytes", 2048.0 * (1.0 + i as f64 * 0.03))
             .add_field("errors", if i % 10 == 0 { 1.0 } else { 0.0 });
             
        network_points.push(point);
    }
    
    db.write_points("network", network_points)?;
    
    info!("写入完成，包含多种序列和字段的示例数据");
    Ok(())
}

