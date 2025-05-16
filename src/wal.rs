use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Mutex,
};

use crate::error::{Error, Result};
use log::{debug, error, info};

pub type Timestamp = u64;
pub type Value = f64;

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

    pub fn append(&self, ts: Timestamp, value: Value) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.write_all(&ts.to_be_bytes())?;
        file.write_all(&value.to_be_bytes())?;
        file.flush()?;
        debug!("WAL 追加写入 ts={}, value={}", ts, value);
        Ok(())
    }

    pub fn batch_append(&self, data: &[(Timestamp, Value)]) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        for &(ts, value) in data {
            file.write_all(&ts.to_be_bytes())?;
            file.write_all(&value.to_be_bytes())?;
        }
        file.flush()?;
        debug!("WAL 批量写入 {} 条数据", data.len());
        Ok(())
    }

    pub fn load(&self) -> Result<BTreeMap<Timestamp, Value>> {
        let mut map = BTreeMap::new();
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                info!("WAL文件不存在，创建新的数据库");
                return Ok(map);
            }
            Err(e) => return Err(Error::IoError(e)),
        };
        
        let mut reader = BufReader::new(file);
        let mut buf = [0u8; 16];
        loop {
            match reader.read_exact(&mut buf) {
                Ok(()) => {
                    let ts = Timestamp::from_be_bytes(buf[0..8].try_into().unwrap());
                    let val = Value::from_be_bytes(buf[8..16].try_into().unwrap());
                    map.insert(ts, val);
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    error!("WAL 读取错误: {}", e);
                    return Err(Error::IoError(e));
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

