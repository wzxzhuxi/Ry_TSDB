use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufWriter, Write, Seek},
    path::{Path, PathBuf},
};

use log::{debug, info, error};
use memmap2::{Mmap, MmapOptions};

use crate::error::{Error, Result};
use crate::gorilla::MultiFieldBlock;
use crate::types::{DataPoint, SeriesKey, QueryFilter, Timestamp};

/// SSTable文件结构：使用Gorilla压缩和内存映射实现零拷贝读取
pub struct SSTable {
    pub path: PathBuf,
    mmap: Option<Mmap>, 
    // 映射到文件中的序列索引，每个序列指向其数据在文件中的位置和长度
    series_index: HashMap<SeriesKey, (usize, usize)>,
}

impl SSTable {
    /// 创建新的SSTable文件，使用Gorilla压缩存储多个序列的数据
    pub fn create(dir: &str, data_by_series: &HashMap<SeriesKey, Vec<DataPoint>>) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let file_id = chrono::Utc::now().timestamp();
        let path = Path::new(dir).join(format!("sstable-{}.db", file_id));
        
        // 预处理每个序列的数据为MultiFieldBlock
        let mut series_blocks: HashMap<SeriesKey, MultiFieldBlock> = HashMap::new();
        
        for (series_key, data_points) in data_by_series {
            let mut block = MultiFieldBlock::new();
            
            for point in data_points {
                block.add_point(point.timestamp, &point.fields);
            }
            
            series_blocks.insert(series_key.clone(), block);
        }
        
        // 写入文件
        let mut file = BufWriter::new(File::create(&path)?);
        
        // 写入序列数量
        let series_count = series_blocks.len() as u32;
        file.write_all(&series_count.to_le_bytes())?;
        
        // 预留序列索引空间，每个序列需要：序列键长度(4) + 序列键 + 位置(8) + 长度(8)
        let index_start_pos = 4; // 序列数量后的位置
        let mut curr_pos = index_start_pos;
        
        // 估算索引大小，先跳过
        for (series_key, _) in &series_blocks {
            // 序列键序列化大小估算：测量名长度(4) + 测量名 + 标签数量(4) + 每个标签(键长度(4) + 键 + 值长度(4) + 值)
            let key_size = 4 + series_key.measurement.len() + 4 + 
                series_key.tags.iter().map(|(k, v)| 4 + k.len() + 4 + v.len()).sum::<usize>();
                
            curr_pos += key_size + 16; // 16 = 位置(8) + 长度(8)
        }
        
        // 跳到数据开始位置
        let data_start_pos = curr_pos;
        file.seek(io::SeekFrom::Start(data_start_pos as u64))?;
        
        // 写入每个序列的数据
        let mut series_positions = HashMap::new();
        for (series_key, block) in &series_blocks {
            let start_pos = file.stream_position()? as usize;
            let compressed_data = block.compress()?;
            file.write_all(&compressed_data)?;
            let end_pos = file.stream_position()? as usize;
            
            series_positions.insert(series_key.clone(), (start_pos, end_pos - start_pos));
        }
        
        // 回到索引开始位置，写入索引
        file.seek(io::SeekFrom::Start(index_start_pos as u64))?;
        
        for (series_key, (pos, len)) in &series_positions {
            // 写入序列键
            let key_bytes = serde_json::to_vec(series_key)?;
            file.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
            file.write_all(&key_bytes)?;
            
            // 写入位置和长度
            file.write_all(&(*pos as u64).to_le_bytes())?;
            file.write_all(&(*len as u64).to_le_bytes())?;
        }
        
        file.flush()?;
        
        // 计算总数据量和压缩率
        let total_points: usize = data_by_series.values().map(|points| points.len()).sum();
        let original_size = total_points * 16; // 每条记录16字节(时间戳8字节 + 值8字节)的简单估计
        let compressed_size = series_positions.values().map(|(_, len)| len).sum::<usize>();
        
        let compression_ratio = if original_size > 0 {
            compressed_size as f64 / original_size as f64
        } else {
            0.0
        };
        
        info!("生成压缩SSTable文件: {:?}, 包含{}个序列, {}条数据点, 压缩率: {:.2}, 压缩后: {}字节", 
              path, series_blocks.len(), total_points, compression_ratio, compressed_size);
        
        // 创建SSTable对象
        Ok(SSTable {
            path,
            mmap: None,
            series_index: series_positions,
        })
    }
    
    /// 打开现有的SSTable文件，读取元数据和索引
    pub fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        if mmap.len() < 4 {
            return Err(Error::DataError("SSTable文件格式错误: 太小".to_string()));
        }
        
        // 读取序列数量
        let series_count = u32::from_le_bytes(mmap[0..4].try_into().unwrap()) as usize;
        
        // 读取序列索引
        let mut offset = 4;
        let mut series_index = HashMap::new();
        
        for _ in 0..series_count {
            if offset + 4 > mmap.len() {
                return Err(Error::DataError("SSTable索引不完整".to_string()));
            }
            
            // 读取序列键长度
            let key_len = u32::from_le_bytes(mmap[offset..offset+4].try_into().unwrap()) as usize;
            offset += 4;
            
            if offset + key_len + 16 > mmap.len() {
                return Err(Error::DataError("SSTable索引损坏".to_string()));
            }
            
            // 读取序列键
            let series_key: SeriesKey = serde_json::from_slice(&mmap[offset..offset+key_len])?;
            offset += key_len;
            
            // 读取位置和长度
            let pos = u64::from_le_bytes(mmap[offset..offset+8].try_into().unwrap()) as usize;
            offset += 8;
            
            let len = u64::from_le_bytes(mmap[offset..offset+8].try_into().unwrap()) as usize;
            offset += 8;
            
            series_index.insert(series_key, (pos, len));
        }
        
        info!("打开SSTable文件: {:?}, 包含{}个序列", path, series_count);
        
        Ok(SSTable {
            path,
            mmap: Some(mmap),
            series_index,
        })
    }
    
    /// 判断查询区间是否与当前文件有交集
    pub fn may_contain(&self, start: Timestamp, end: Timestamp) -> bool {
        true  // 简化版实现，默认可能包含
    }

    /// 查询满足过滤条件的数据
    pub fn query(&self, filter: &QueryFilter) -> Result<HashMap<SeriesKey, HashMap<String, Vec<(Timestamp, f64)>>>> {
        let (start_time, end_time) = filter.time_range;
        let mut results = HashMap::new();
        
        let mmap = match &self.mmap {
            Some(m) => m,
            None => return Err(Error::MemMapError("SSTable未加载到内存映射".to_string())),
        };
        
        // 遍历索引，查找匹配的序列
        for (series_key, (pos, len)) in &self.series_index {
            // 如果有指定测量名，检查是否匹配
            if let Some(ref measurement) = filter.measurement {
                if series_key.measurement != *measurement {
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
            
            // 检查数据是否在范围内
            if *pos + *len > mmap.len() {
                return Err(Error::DataError("SSTable数据指针超出文件范围".to_string()));
            }
            
            // 读取和解压序列数据
            let data = &mmap[*pos..*pos + *len];
            let multi_block = match MultiFieldBlock::decompress(data) {
                Ok(block) => block,
                Err(e) => {
                    error!("解压序列数据失败: {:?}", e);
                    continue;
                }
            };
            
            // 查询符合时间范围的数据点
            let field_results = multi_block.query(start_time, end_time, &filter.fields);
            
            if !field_results.is_empty() {
                results.insert(series_key.clone(), field_results);
            }
        }
        
        debug!("SSTable查询返回 {} 个匹配序列", results.len());
        Ok(results)
    }
}

