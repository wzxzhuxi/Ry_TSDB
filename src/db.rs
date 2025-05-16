use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use log::{debug, error, info};

use crate::{
    error::Result,
    sstable::SSTable,
    wal::{Timestamp, Value, Wal},
};

/// 简易LSM-Tree TSDB结构
pub struct SimpleTSDB {
    memtable: Arc<Mutex<BTreeMap<Timestamp, Value>>>,
    wal: Arc<Wal>,
    sstables: Arc<Mutex<Vec<SSTable>>>,
    sstable_dir: String,
    wal_path: String,
    memtable_size_threshold: usize,
}

impl SimpleTSDB {
    pub fn open(config: DbConfig) -> Result<Self> {
        // 创建目录
        std::fs::create_dir_all(&config.sstable_dir)?;
        
        // 初始化WAL并恢复MemTable
        let wal = Arc::new(Wal::open(&config.wal_path)?);
        let memtable = wal.load()?;

        // 加载现有的SSTable文件
        let mut sstables = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&config.sstable_dir) {
            for entry in entries.flatten() {
                if entry.path().extension().and_then(|s| s.to_str()) == Some("db") {
                    match SSTable::open(entry.path()) {
                        Ok(sst) => sstables.push(sst),
                        Err(e) => error!("加载SSTable失败: {:?}", e),
                    }
                }
            }
        }

        let db = SimpleTSDB {
            memtable: Arc::new(Mutex::new(memtable)),
            wal: Arc::clone(&wal),
            sstables: Arc::new(Mutex::new(sstables)),
            sstable_dir: config.sstable_dir.clone(),
            wal_path: config.wal_path.clone(),
            memtable_size_threshold: config.memtable_size_threshold,
        };

        // 启动后台刷盘线程
        {
            let memtable = Arc::clone(&db.memtable);
            let wal = Arc::clone(&wal);
            let sstables = Arc::clone(&db.sstables);
            let sstable_dir = config.sstable_dir.clone();
            let threshold = config.memtable_size_threshold;
            
            thread::spawn(move || loop {
                thread::sleep(Duration::from_secs(5));
                let mut mem = memtable.lock().unwrap();
                if mem.len() >= threshold {
                    info!("MemTable达到阈值，开始刷盘");
                    match SSTable::create(&sstable_dir, &mem) {
                        Ok(sst) => {
                            sstables.lock().unwrap().push(sst);
                            mem.clear();
                            if let Err(e) = wal.clear() {
                                error!("清空WAL失败: {:?}", e);
                            }
                            info!("刷盘完成");
                        }
                        Err(e) => error!("刷盘失败: {:?}", e),
                    }
                }
            });
        }

        info!("TSDB初始化完成，加载了{}个SSTable文件", db.sstables.lock().unwrap().len());
        Ok(db)
    }

    /// 写入单条数据
    pub fn put(&self, ts: Timestamp, value: Value) -> Result<()> {
        self.wal.append(ts, value)?;
        let mut mem = self.memtable.lock().unwrap();
        mem.insert(ts, value);
        debug!("写入MemTable ts={}, value={}", ts, value);
        Ok(())
    }

    /// 批量写入数据
    pub fn batch_put(&self, data: &[(Timestamp, Value)]) -> Result<()> {
        self.wal.batch_append(data)?;
        let mut mem = self.memtable.lock().unwrap();
        for &(ts, value) in data {
            mem.insert(ts, value);
        }
        debug!("批量写入{}条数据到MemTable", data.len());
        Ok(())
    }

    /// 查询区间数据
    pub fn query(&self, start: Timestamp, end: Timestamp) -> Result<Vec<(Timestamp, Value)>> {
        let mut result = Vec::new();

        // 先查MemTable
        {
            let mem = self.memtable.lock().unwrap();
            for (&ts, &val) in mem.range(start..=end) {
                result.push((ts, val));
            }
        }

        // 查询SSTable
        let sstables = self.sstables.lock().unwrap();
        for sst in sstables.iter() {
            if sst.may_contain(start, end) {
                let mut res = sst.query(start, end)?;
                result.append(&mut res);
            }
        }

        // 合并结果，去重
        result.sort_by_key(|&(ts, _)| ts);
        result.dedup_by_key(|&mut (ts, _)| ts);

        info!("查询区间[{}, {}]返回{}条数据", start, end, result.len());
        Ok(result)
    }
    
    /// 获取压缩和存储统计信息
    pub fn get_stats(&self) -> Result<DbStats> {
        let sstables = self.sstables.lock().unwrap();
        let mut total_files = 0;
        let mut total_size = 0;
        
        for entry in std::fs::read_dir(&self.sstable_dir)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("db") {
                total_files += 1;
                total_size += entry.metadata()?.len();
            }
        }
        
        let mem_size = {
            let mem = self.memtable.lock().unwrap();
            mem.len()
        };
        
        Ok(DbStats {
            sstable_count: total_files,
            total_disk_size: total_size,
            memtable_records: mem_size,
        })
    }
}

pub struct DbConfig {
    pub sstable_dir: String,
    pub wal_path: String,
    pub memtable_size_threshold: usize,
}

impl Default for DbConfig {
    fn default() -> Self {
        DbConfig {
            sstable_dir: "./data/sstable".to_string(),
            wal_path: "./data/wal.log".to_string(),
            memtable_size_threshold: 1000,
        }
    }
}

pub struct DbStats {
    pub sstable_count: usize,
    pub total_disk_size: u64,
    pub memtable_records: usize,
}

