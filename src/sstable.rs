use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use log::{debug, info};
use memmap2::{Mmap, MmapOptions};

use crate::error::{Error, Result};
use crate::gorilla::TimeSeriesBlock;
use crate::wal::{Timestamp, Value};

/// SSTable文件结构：使用Gorilla压缩和内存映射实现零拷贝读取
pub struct SSTable {
    pub path: PathBuf,
    mmap: Option<Mmap>, // 内存映射用于零拷贝
    min_ts: Timestamp,  // 文件中的最小时间戳
    max_ts: Timestamp,  // 文件中的最大时间戳
}

impl SSTable {
    /// 创建新的SSTable文件，使用Gorilla压缩数据
    pub fn create(dir: &str, data: &BTreeMap<Timestamp, Value>) -> Result<Self> {
        fs::create_dir_all(dir)?;
        let file_id = chrono::Utc::now().timestamp();
        let path = Path::new(dir).join(format!("sstable-{}.db", file_id));
        
        // 获取最小和最大时间戳
        let min_ts = *data.keys().next().unwrap_or(&0);
        let max_ts = *data.keys().next_back().unwrap_or(&0);
        
        // 将数据转换为TimeSeriesBlock
        let mut block = TimeSeriesBlock::new();
        for (&ts, &val) in data.iter() {
            block.add_point(ts, val);
        }
        
        // 压缩数据
        let compressed_data = block.compress()?;
        
        // 写入文件
        let mut file = BufWriter::new(File::create(&path)?);
        
        // 写入元数据：最小TS、最大TS
        file.write_all(&min_ts.to_le_bytes())?;
        file.write_all(&max_ts.to_le_bytes())?;
        
        // 写入压缩数据长度和数据
        let compressed_len = compressed_data.len() as u32;
        file.write_all(&compressed_len.to_le_bytes())?;
        file.write_all(&compressed_data)?;
        file.flush()?;
        
        let original_size = data.len() * 16; // 每条记录16字节(8字节ts + 8字节value)
        let compression_ratio = if original_size > 0 {
            compressed_data.len() as f64 / original_size as f64
        } else {
            0.0
        };
        
        info!("生成压缩SSTable文件: {:?}, 压缩率: {:.2}, 原始大小: {}字节, 压缩后: {}字节", 
              path, compression_ratio, original_size, compressed_data.len());
        
        let sstable = SSTable {
            path,
            mmap: None,
            min_ts,
            max_ts,
        };
        
        Ok(sstable)
    }
    
    /// 打开现有的SSTable文件，使用内存映射实现零拷贝访问
    pub fn open(path: PathBuf) -> Result<Self> {
        let file = File::open(&path)?;
        
        // 读取元数据
        let mut reader = io::BufReader::new(&file);
        let mut buf = [0u8; 8];
        
        reader.read_exact(&mut buf)?;
        let min_ts = u64::from_le_bytes(buf);
        
        reader.read_exact(&mut buf)?;
        let max_ts = u64::from_le_bytes(buf);
        
        // 使用内存映射实现零拷贝
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        info!("打开SSTable文件: {:?}, 时间范围: [{}, {}]", path, min_ts, max_ts);
        
        Ok(SSTable {
            path,
            mmap: Some(mmap),
            min_ts,
            max_ts,
        })
    }
    
    /// 判断查询区间是否与当前文件有交集
    pub fn may_contain(&self, start: Timestamp, end: Timestamp) -> bool {
        !(end < self.min_ts || start > self.max_ts)
    }

    /// 查询区间数据，使用零拷贝技术
    pub fn query(&self, start: Timestamp, end: Timestamp) -> Result<Vec<(Timestamp, Value)>> {
        let mut results = Vec::new();
        
        // 检查区间是否有交集
        if !self.may_contain(start, end) {
            return Ok(results);
        }
        
        // 懒加载mmap，如果没有初始化，则进行加载
        let mmap = match &self.mmap {
            Some(m) => m,
            None => {
                return Err(Error::MemMapError(
                    "SSTable未加载到内存映射".to_string(),
                ));
            }
        };
        
        if mmap.len() < 20 {
            return Err(Error::DataError("SSTable文件格式错误".to_string()));
        }
        
        // 读取元数据和压缩数据
        let _min_ts = u64::from_le_bytes(mmap[0..8].try_into().unwrap());
        let _max_ts = u64::from_le_bytes(mmap[8..16].try_into().unwrap());
        let compressed_len = u32::from_le_bytes(mmap[16..20].try_into().unwrap()) as usize;
        
        if 20 + compressed_len > mmap.len() {
            return Err(Error::DataError("压缩数据长度超出文件大小".to_string()));
        }
        
        // 零拷贝方式访问压缩数据 - 直接从内存映射中读取，不复制
        let compressed_data = &mmap[20..20 + compressed_len];
        
        // 解压并查询
        let block = match TimeSeriesBlock::decompress(compressed_data) {
            Ok(b) => b,
            Err(e) => return Err(Error::CompressionError(format!("解压失败: {}", e))),
        };
        
        // 查询指定区间
        results = block.query(start, end);
        
        debug!("SSTable查询 {:?} 返回 {} 条数据", self.path, results.len());
        Ok(results)
    }
}

