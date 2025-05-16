use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use crate::error::{Error, Result};
use crate::types::{DataPoint, Timestamp, FieldValue};
use log::{debug, info, warn};

/// 写前日志，确保写入操作的持久化
pub struct Wal {
    file: Mutex<BufWriter<File>>,
    path: String,
}

impl Wal {
    pub fn open(path: &str) -> Result<Self> {
        fs::create_dir_all(Path::new(path).parent().unwrap())?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        info!("WAL 打开: {}", path);
        Ok(Wal {
            file: Mutex::new(BufWriter::new(file)),
            path: path.to_string(),
        })
    }

    // 向WAL写入包含标签和字段的数据点
    pub fn append_data_point(&self, point: &DataPoint) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        
        // 写入时间戳
        file.write_all(&point.timestamp.to_be_bytes())?;
        
        // 写入标签数量和标签
        let tag_count = point.tags.len() as u32;
        file.write_all(&tag_count.to_be_bytes())?;
        
        for (key, value) in &point.tags {
            // 写入键长度和键
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u32;
            file.write_all(&key_len.to_be_bytes())?;
            file.write_all(key_bytes)?;
            
            // 写入值长度和值
            let val_bytes = value.as_bytes();
            let val_len = val_bytes.len() as u32;
            file.write_all(&val_len.to_be_bytes())?;
            file.write_all(val_bytes)?;
        }
        
        // 写入字段数量和字段
        let field_count = point.fields.len() as u32;
        file.write_all(&field_count.to_be_bytes())?;
        
        for (key, value) in &point.fields {
            // 写入键长度和键
            let key_bytes = key.as_bytes();
            let key_len = key_bytes.len() as u32;
            file.write_all(&key_len.to_be_bytes())?;
            file.write_all(key_bytes)?;
            
            // 写入值
            file.write_all(&value.to_be_bytes())?;
        }
        
        file.flush()?;
        debug!("WAL 追加写入数据点: ts={}, 标签数={}, 字段数={}", 
               point.timestamp, tag_count, field_count);
        Ok(())
    }

    // 批量写入数据点
    pub fn batch_append_data_points(&self, points: &[DataPoint]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        
        for point in points {
            // 写入时间戳
            file.write_all(&point.timestamp.to_be_bytes())?;
            
            // 写入标签数量和标签
            let tag_count = point.tags.len() as u32;
            file.write_all(&tag_count.to_be_bytes())?;
            
            for (key, value) in &point.tags {
                // 写入键长度和键
                let key_bytes = key.as_bytes();
                let key_len = key_bytes.len() as u32;
                file.write_all(&key_len.to_be_bytes())?;
                file.write_all(key_bytes)?;
                
                // 写入值长度和值
                let val_bytes = value.as_bytes();
                let val_len = val_bytes.len() as u32;
                file.write_all(&val_len.to_be_bytes())?;
                file.write_all(val_bytes)?;
            }
            
            // 写入字段数量和字段
            let field_count = point.fields.len() as u32;
            file.write_all(&field_count.to_be_bytes())?;
            
            for (key, value) in &point.fields {
                // 写入键长度和键
                let key_bytes = key.as_bytes();
                let key_len = key_bytes.len() as u32;
                file.write_all(&key_len.to_be_bytes())?;
                file.write_all(key_bytes)?;
                
                // 写入值
                file.write_all(&value.to_be_bytes())?;
            }
        }
        
        file.flush()?;
        debug!("WAL 批量追加写入 {} 条数据点", points.len());
        Ok(())
    }

    // 兼容旧接口
    pub fn append(&self, ts: Timestamp, value: FieldValue) -> Result<()> {
        let mut point = DataPoint::new(ts);
        point.add_field("value", value);
        self.append_data_point(&point)
    }

    // 兼容旧接口
    pub fn batch_append(&self, data: &[(Timestamp, FieldValue)]) -> Result<()> {
        let points: Vec<DataPoint> = data.iter()
            .map(|&(ts, value)| {
                let mut point = DataPoint::new(ts);
                point.add_field("value", value);
                point
            })
            .collect();
        
        self.batch_append_data_points(&points)
    }

    // 加载WAL，恢复数据
    pub fn load_points(&self) -> Result<HashMap<String, Vec<DataPoint>>> {
        let mut result = HashMap::new();
        
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("WAL文件不存在，创建新的数据库");
                return Ok(result);
            }
            Err(e) => return Err(Error::IoError(e)),
        };
        
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        
        let mut total_points = 0;
        
        loop {
            // 尝试读取时间戳，如果读取失败则退出循环
            let mut ts_buf = [0u8; 8];
            match reader.read_exact(&mut ts_buf) {
                Ok(()) => {},
                Err(e) => {
                    // 如果是意外EOF，认为是读取结束，记录警告并中断循环
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取时遇到意外 EOF，可能是文件不完整，已读取 {} 条数据点", total_points);
                        break;
                    }
                    // 其他错误则返回
                    return Err(Error::IoError(e));
                }
            }
            
            let timestamp = u64::from_le_bytes(ts_buf);
            let mut point = DataPoint::new(timestamp);
            
            // 读取标签，增加错误处理
            let mut tag_count_buf = [0u8; 4];
            match reader.read_exact(&mut tag_count_buf) {
                Ok(()) => {},
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取标签计数时遇到意外 EOF");
                        break;
                    }
                    return Err(Error::IoError(e));
                }
            }
            
            let tag_count = u32::from_le_bytes(tag_count_buf);
            
            let mut read_failed = false;
            for _ in 0..tag_count {
                // 读取键长度时增加错误处理
                let mut key_len_buf = [0u8; 4];
                if let Err(e) = reader.read_exact(&mut key_len_buf) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取标签键长度时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let key_len = u32::from_le_bytes(key_len_buf) as usize;
                
                buffer.clear();
                buffer.resize(key_len, 0);
                if let Err(e) = reader.read_exact(&mut buffer) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取标签键数据时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let key = match String::from_utf8(buffer.clone()) {
                    Ok(k) => k,
                    Err(_) => {
                        warn!("WAL 文件包含无效的UTF-8标签键");
                        continue;
                    }
                };
                
                // 读取值
                let mut val_len_buf = [0u8; 4];
                if let Err(e) = reader.read_exact(&mut val_len_buf) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取标签值长度时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let val_len = u32::from_le_bytes(val_len_buf) as usize;
                
                buffer.clear();
                buffer.resize(val_len, 0);
                if let Err(e) = reader.read_exact(&mut buffer) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取标签值数据时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let value = match String::from_utf8(buffer.clone()) {
                    Ok(v) => v,
                    Err(_) => {
                        warn!("WAL 文件包含无效的UTF-8标签值");
                        continue;
                    }
                };
                
                point.add_tag(key, value);
            }
            
            if read_failed {
                break;
            }
            
            // 读取字段
            let mut field_count_buf = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut field_count_buf) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    warn!("WAL 文件读取字段计数时遇到意外 EOF");
                    break;
                }
                return Err(Error::IoError(e));
            }
            
            let field_count = u32::from_le_bytes(field_count_buf);
            
            for _ in 0..field_count {
                // 读取字段键
                let mut key_len_buf = [0u8; 4];
                if let Err(e) = reader.read_exact(&mut key_len_buf) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取字段键长度时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let key_len = u32::from_le_bytes(key_len_buf) as usize;
                
                buffer.clear();
                buffer.resize(key_len, 0);
                if let Err(e) = reader.read_exact(&mut buffer) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取字段键数据时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let key = match String::from_utf8(buffer.clone()) {
                    Ok(k) => k,
                    Err(_) => {
                        warn!("WAL 文件包含无效的UTF-8字段键");
                        continue;
                    }
                };
                
                // 读取字段值
                let mut val_buf = [0u8; 8];
                if let Err(e) = reader.read_exact(&mut val_buf) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        warn!("WAL 文件读取字段值时遇到意外 EOF");
                        read_failed = true;
                        break;
                    }
                    return Err(Error::IoError(e));
                }
                
                let value = f64::from_le_bytes(val_buf);
                point.add_field(key, value);
            }
            
            if read_failed {
                break;
            }
            
            // 保存数据点，使用标签组合作为key
            let series_key = point.tags.get("measurement")
                .cloned().unwrap_or_else(|| "default".to_string());
            
            let points = result.entry(series_key).or_insert_with(Vec::new);
            points.push(point);
            total_points += 1;
        }
        
        info!("WAL 加载完成，恢复 {} 个序列，共 {} 条数据点", result.len(), total_points);
        Ok(result)
    }

    // 兼容旧接口
    pub fn load(&self) -> Result<BTreeMap<Timestamp, FieldValue>> {
        let mut map = BTreeMap::new();
        let points_by_series = self.load_points()?;
        
        // 合并所有序列的点，简单提取"value"字段
        for (_, points) in points_by_series {
            for point in points {
                if let Some(&value) = point.fields.get("value") {
                    map.insert(point.timestamp, value);
                }
            }
        }
        
        info!("WAL 加载完成，恢复 {} 条数据", map.len());
        Ok(map)
    }

    pub fn clear(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.get_mut().set_len(0)?;
        file.get_mut().seek(SeekFrom::Start(0))?;
        file.flush()?;
        info!("WAL 文件清空");
        Ok(())
    }
}

