use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use log::{debug, error, info};

use crate::{
    error::Result,
    sstable::SSTable,
    wal::Wal,
    types::{Timestamp, DataPoint, SeriesKey, QueryFilter},
};

/// 简易LSM-Tree TSDB结构
pub struct SimpleTSDB {
    // 按系列组织的内存表
    memtable: Arc<Mutex<HashMap<SeriesKey, Vec<DataPoint>>>>,
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
        let points_by_series = wal.load_points()?;
        
        // 转换为序列索引的内存表格式
        let mut memtable: HashMap<SeriesKey, Vec<DataPoint>> = HashMap::new();
        for (measurement, points) in points_by_series {
            for point in points {
                // 构建序列键
                let mut series_key = SeriesKey::new(&measurement);
                for (k, v) in &point.tags {
                    series_key.add_tag(k, v);
                }
                
                // 添加到内存表
                let series_points = memtable.entry(series_key).or_insert_with(Vec::new);
                series_points.push(point);
            }
        }

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
                let total_points: usize = mem.values().map(|points| points.len()).sum();
                
                if total_points >= threshold {
                    info!("MemTable达到阈值，开始刷盘 ({} 条数据点)", total_points);
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

    /// 写入带标签的数据点
    pub fn write_point(&self, measurement: &str, point: DataPoint) -> Result<()> {
        // 构建序列键
        let mut series_key = SeriesKey::new(measurement);
        for (key, value) in &point.tags {
            series_key.add_tag(key.clone(), value.clone());
        }
        
        // 写入WAL
        self.wal.append_data_point(&point)?;

        // 写入内存表
        let mut mem = self.memtable.lock().unwrap();
        let points = mem.entry(series_key).or_insert_with(Vec::new);
        let ts = point.timestamp; // 先保存时间戳，避免移动后使用
        points.push(point);
        
        debug!("写入数据点到MemTable, 时间戳={}", ts);
        Ok(())
    }
    
    /// 批量写入数据点
    pub fn write_points(&self, measurement: &str, points: Vec<DataPoint>) -> Result<()> {
        // 写入WAL
        self.wal.batch_append_data_points(&points)?;
        
        // 写入内存表
        let mut mem = self.memtable.lock().unwrap();
        
        for point in points {
            // 构建序列键
            let mut series_key = SeriesKey::new(measurement);
            for (key, value) in &point.tags {
                series_key.add_tag(key.clone(), value.clone());
            }
            
            let series_points = mem.entry(series_key).or_insert_with(Vec::new);
            series_points.push(point);
        }
        
        debug!("批量写入数据点到MemTable");
        Ok(())
    }
    
    /// 查询接口
    pub fn query(&self, filter: QueryFilter) -> Result<HashMap<SeriesKey, HashMap<String, Vec<(Timestamp, f64)>>>> {
        let mut result = HashMap::new();
        
        // 查询内存表
        {
            let mem = self.memtable.lock().unwrap();
            for (series_key, points) in mem.iter() {
                // 如果指定了measurement，检查是否匹配
                if let Some(ref m) = filter.measurement {
                    if series_key.measurement != *m {
                        continue;
                    }
                }
                
                // 检查标签是否匹配
                let mut match_tags = true;
                for (tag_key, tag_value) in &filter.tags {
                    match series_key.tags.get(tag_key) {
                        Some(value) if value == tag_value => continue,
                        _ => {
                            match_tags = false;
                            break;
                        }
                    }
                }
                
                if !match_tags {
                    continue;
                }
                
                // 系列匹配，提取时间范围内的点
                let (start_time, end_time) = filter.time_range;
                let mut field_points: HashMap<String, Vec<(Timestamp, f64)>> = HashMap::new();
                
                for point in points {
                    if point.timestamp >= start_time && point.timestamp <= end_time {
                        // 提取请求的字段，或全部字段
                        let fields_to_extract = if filter.fields.is_empty() {
                            point.fields.keys().cloned().collect()
                        } else {
                            filter.fields.iter().filter(|f| point.fields.contains_key(*f)).cloned().collect::<Vec<_>>()
                        };
                        
                        for field_name in fields_to_extract {
                            if let Some(value) = point.fields.get(&field_name) {
                                let field_series = field_points.entry(field_name).or_insert_with(Vec::new);
                                field_series.push((point.timestamp, *value));
                            }
                        }
                    }
                }
                
                if !field_points.is_empty() {
                    result.insert(series_key.clone(), field_points);
                }
            }
        }
        
        // 查询SSTable
        let sstables = self.sstables.lock().unwrap();
        for sst in sstables.iter() {
            let sst_results = sst.query(&filter)?;
            
            // 合并结果
            for (series_key, fields) in sst_results {
                let entry = result.entry(series_key).or_insert_with(HashMap::new);
                
                for (field_name, mut points) in fields {
                    let field_entry = entry.entry(field_name).or_insert_with(Vec::new);
                    field_entry.append(&mut points);
                }
            }
        }
        
        // 对每个字段的点进行排序和去重
        for (_, fields) in result.iter_mut() {
            for (_, points) in fields.iter_mut() {
                points.sort_by_key(|&(ts, _)| ts);
                points.dedup_by_key(|&mut (ts, _)| ts);
            }
        }
        
        info!("查询返回 {} 个序列", result.len());
        Ok(result)
    }
    
    /// 兼容旧接口：单值写入
    pub fn put(&self, ts: Timestamp, value: f64) -> Result<()> {
        let mut point = DataPoint::new(ts);
        point.add_field("value", value);
        
        // 删除未使用变量
        self.write_point("default", point)
    }
    
    /// 兼容旧接口：批量单值写入
    pub fn batch_put(&self, data: &[(Timestamp, f64)]) -> Result<()> {
        let points: Vec<DataPoint> = data.iter()
            .map(|&(ts, value)| {
                let mut point = DataPoint::new(ts);
                point.add_field("value", value);
                point
            })
            .collect();
        
        self.write_points("default", points)
    }
    
    /// 兼容旧接口：单值查询
    pub fn legacy_query(&self, start: Timestamp, end: Timestamp) -> Result<Vec<(Timestamp, f64)>> {
        let filter = QueryFilter::new(start, end)
            .measurement("default")
            .add_field("value");
        
        let results = self.query(filter)?;
        
        let mut points = Vec::new();
        for (_, fields) in results {
            if let Some(field_points) = fields.get("value") {
                points.extend_from_slice(field_points);
            }
        }
        
        // 排序和去重
        points.sort_by_key(|&(ts, _)| ts);
        points.dedup_by_key(|&mut (ts, _)| ts);
        
        info!("查询区间[{}, {}]返回{}条数据", start, end, points.len());
        Ok(points)
    }
    
    /// 获取压缩和存储统计信息
    pub fn get_stats(&self) -> Result<DbStats> {
        let _sstables = self.sstables.lock().unwrap();
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
            mem.values().map(|points| points.len()).sum::<usize>()
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

