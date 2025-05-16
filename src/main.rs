mod db;
mod error;
mod gorilla;
mod sstable;
mod wal;
mod server;

use std::thread;
use std::time::Duration;
use log::info;
use db::{DbConfig, SimpleTSDB};
use server::TsdbServer;
use std::sync::Arc;

#[tokio::main]
async fn main() -> error::Result<()> {
    // 初始化日志系统
    env_logger::init();
    
    info!("启动LSM-Tree TSDB（Gorilla压缩 + 零拷贝技术）");
    
    // 配置数据库
    let config = DbConfig {
        sstable_dir: "./data/sstable".to_string(),
        wal_path: "./data/wal.log".to_string(),
        memtable_size_threshold: 1000,
    };
    
    // 打开数据库
    let db = SimpleTSDB::open(config)?;
    
    // 写入示例数据
    write_sample_data(&db)?;
    
    // 等待后台刷盘
    thread::sleep(Duration::from_secs(1));
    
    // 查询示例
    let base_ts = 1622000000;
    query_example(&db, base_ts)?;
    
    // 输出统计信息
    let stats = db.get_stats()?;
    info!("数据库统计: {} 个SSTable文件, 磁盘占用: {} 字节, MemTable记录数: {}", 
    stats.sstable_count, stats.total_disk_size, stats.memtable_records);
    
    // 创建并启动服务器，监听6364端口
    let server = TsdbServer::new(Arc::new(db), "127.0.0.1:6364".to_string());
    info!("服务器将在 127.0.0.1:6364 监听请求");
    
    // 启动服务器（这会阻塞主线程）
    server.run().await?;

    Ok(())
}

fn write_sample_data(db: &SimpleTSDB) -> error::Result<()> {
    let base_ts = 1622000000;
    
    // 写入一些规律的CPU数据
    info!("写入示例数据...");
    for i in 0..100 {
        db.put(base_ts + i * 60, 25.0 + (i as f64 % 10.0))?;
    }
    
    // 批量写入一些内存数据
    let mut batch = Vec::new();
    for i in 0..200 {
        batch.push((base_ts + 3000 + i * 30, 8192.0 + (i as f64 * 10.0)));
    }
    db.batch_put(&batch)?;
    
    info!("写入完成，总计 {} 条记录", 100 + batch.len());
    Ok(())
}

fn query_example(db: &SimpleTSDB, base_ts: u64) -> error::Result<()> {
    // 查询CPU数据
    info!("查询CPU数据区间...");
    let results = db.query(base_ts, base_ts + 3000)?;
    println!("CPU数据查询结果（前10条）：");
    for (i, (ts, val)) in results.iter().take(10).enumerate() {
        println!("#{}: ts: {}, value: {}", i+1, ts, val);
    }
    
    // 查询内存数据
    info!("查询内存数据区间...");
    let results = db.query(base_ts + 3000, base_ts + 5000)?;
    println!("\n内存数据查询结果（前10条）：");
    for (i, (ts, val)) in results.iter().take(10).enumerate() {
        println!("#{}: ts: {}, value: {}", i+1, ts, val);
    }
    
    // 查询混合区间
    info!("查询混合区间...");
    let results = db.query(base_ts + 2500, base_ts + 4500)?;
    println!("\n混合区间查询结果（前10条）：");
    for (i, (ts, val)) in results.iter().take(10).enumerate() {
        println!("#{}: ts: {}, value: {}", i+1, ts, val);
    }
    
    Ok(())
}

