use std::io::{self, Read, Write};

/// 按位写入数据的工具类
pub struct BitWriter<W: Write> {
    writer: W,
    buffer: u64,
    bits_in_buffer: u8,
}

impl<W: Write> BitWriter<W> {
    pub fn new(writer: W) -> Self {
        BitWriter {
            writer,
            buffer: 0,
            bits_in_buffer: 0,
        }
    }

    /// 写入指定位数的比特
    pub fn write_bits(&mut self, value: u64, bits: u8) -> io::Result<()> {
        if bits == 0 {
            return Ok(());
        }

        // 添加到缓冲区
        self.buffer |= (value & ((1 << bits) - 1)) << self.bits_in_buffer;
        self.bits_in_buffer += bits;

        // 当缓冲区超过8位，写入到底层流
        while self.bits_in_buffer >= 8 {
            let byte = (self.buffer & 0xFF) as u8;
            self.writer.write_all(&[byte])?;
            self.buffer >>= 8;
            self.bits_in_buffer -= 8;
        }

        Ok(())
    }

    /// 完成写入，将剩余的位刷到底层流
    pub fn flush(&mut self) -> io::Result<()> {
        if self.bits_in_buffer > 0 {
            let byte = (self.buffer & 0xFF) as u8;
            self.writer.write_all(&[byte])?;
            self.buffer = 0;
            self.bits_in_buffer = 0;
        }
        self.writer.flush()
    }
    
    /// 获取底层writer的引用
    pub fn get_ref(&self) -> &W {
        &self.writer
    }
    
    /// 获取底层writer的可变引用
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }
    
    /// 消耗BitWriter，返回底层writer
    pub fn into_inner(self) -> W {
        self.writer
    }
}

/// 按位从源流读取数据的工具类
pub struct BitReader<R: Read> {
    reader: R,
    buffer: u64,
    bits_in_buffer: u8,
}

impl<R: Read> BitReader<R> {
    pub fn new(reader: R) -> Self {
        BitReader {
            reader,
            buffer: 0,
            bits_in_buffer: 0,
        }
    }

    /// 读取指定位数的比特
    pub fn read_bits(&mut self, bits: u8) -> io::Result<u64> {
        if bits == 0 {
            return Ok(0);
        }

        // 确保缓冲区有足够的位
        while self.bits_in_buffer < bits {
            let mut byte = [0u8; 1];
            match self.reader.read_exact(&mut byte) {
                Ok(()) => {
                    self.buffer |= (byte[0] as u64) << self.bits_in_buffer;
                    self.bits_in_buffer += 8;
                }
                Err(e) => {
                    if self.bits_in_buffer == 0 {
                        return Err(e);
                    }
                    break; // 读不到更多数据但缓冲区仍有数据
                }
            }
        }

        // 提取需要的位
        let mask = (1 << bits) - 1;
        let value = self.buffer & mask;
        self.buffer >>= bits;
        self.bits_in_buffer -= bits;

        Ok(value)
    }

    /// 读取一个比特
    pub fn read_bit(&mut self) -> io::Result<bool> {
        Ok(self.read_bits(1)? == 1)
    }
    
    /// 获取底层reader的引用
    pub fn get_ref(&self) -> &R {
        &self.reader
    }
    
    /// 获取底层reader的可变引用
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.reader
    }
    
    /// 消耗BitReader，返回底层reader
    pub fn into_inner(self) -> R {
        self.reader
    }
}

/// Gorilla编码器实现
pub struct GorillaEncoder<W: Write> {
    bit_writer: BitWriter<W>,
    first_timestamp: u64,
    prev_timestamp: u64,
    prev_delta: i64,
    prev_value: f64,
    first_value: bool,
}

impl<W: Write> GorillaEncoder<W> {
    pub fn new(writer: W) -> Self {
        GorillaEncoder {
            bit_writer: BitWriter::new(writer),
            first_timestamp: 0,
            prev_timestamp: 0,
            prev_delta: 0,
            prev_value: 0.0,
            first_value: true,
        }
    }

    /// 压缩一个数据点
    pub fn encode(&mut self, timestamp: u64, value: f64) -> io::Result<()> {
        // 如果是第一个点，完整存储时间戳和值
        if self.first_value {
            self.bit_writer.write_bits(timestamp, 64)?;
            self.bit_writer.write_bits(f64::to_bits(value), 64)?;
            
            self.first_timestamp = timestamp;
            self.prev_timestamp = timestamp;
            self.prev_value = value;
            self.first_value = false;
            
            return Ok(());
        }

        // 编码时间戳 - delta-of-delta
        let delta = timestamp as i64 - self.prev_timestamp as i64;
        let delta_of_delta = delta - self.prev_delta;
        
        // 根据delta-of-delta大小选择不同的编码
        if delta_of_delta == 0 {
            // 不变，用1位表示
            self.bit_writer.write_bits(0, 1)?;
        } else if delta_of_delta >= -63 && delta_of_delta <= 64 {
            // 小范围变化，用9位表示
            self.bit_writer.write_bits(0b10, 2)?;
            self.bit_writer.write_bits((delta_of_delta & 0x7F) as u64, 7)?;
        } else if delta_of_delta >= -255 && delta_of_delta <= 256 {
            // 中等范围变化，用12位表示
            self.bit_writer.write_bits(0b110, 3)?;
            self.bit_writer.write_bits((delta_of_delta & 0x1FF) as u64, 9)?;
        } else if delta_of_delta >= -2047 && delta_of_delta <= 2048 {
            // 较大范围变化，用16位表示
            self.bit_writer.write_bits(0b1110, 4)?;
            self.bit_writer.write_bits((delta_of_delta & 0xFFF) as u64, 12)?;
        } else {
            // 大范围变化，用36位表示
            self.bit_writer.write_bits(0b1111, 4)?;
            self.bit_writer.write_bits((delta_of_delta & 0xFFFFFFFF) as u64, 32)?;
        }

        self.prev_delta = delta;
        self.prev_timestamp = timestamp;

        // 编码浮点值 - XOR
        let value_bits = f64::to_bits(value);
        let prev_value_bits = f64::to_bits(self.prev_value);
        let xor = value_bits ^ prev_value_bits;

        if xor == 0 {
            // 值相同，使用1位0表示
            self.bit_writer.write_bits(0, 1)?;
        } else {
            // 值不同
            let leading_zeros = xor.leading_zeros() as u8;
            let trailing_zeros = xor.trailing_zeros() as u8;
            
            // 计算有意义的位
            let significant_bits = 64 - leading_zeros - trailing_zeros;
            
            // 写入1前缀，表示值不同
            self.bit_writer.write_bits(1, 1)?;
            
            // 写入前导零数量（5位）
            self.bit_writer.write_bits(leading_zeros as u64, 5)?;
            
            // 写入有意义位的数量（6位）
            self.bit_writer.write_bits(significant_bits as u64, 6)?;
            
            // 写入有意义的位
            let meaningful_bits = xor >> trailing_zeros;
            self.bit_writer.write_bits(meaningful_bits, significant_bits)?;
        }

        self.prev_value = value;
        
        Ok(())
    }

    /// 完成编码，刷新缓冲区
    pub fn close(mut self) -> io::Result<W> {
        self.bit_writer.flush()?;
        Ok(self.bit_writer.into_inner())
    }
}

/// Gorilla解码器实现
pub struct GorillaDecoder<R: Read> {
    bit_reader: BitReader<R>,
    first_timestamp: u64,
    prev_timestamp: u64,
    prev_delta: i64,
    prev_value: f64,
    first_value: bool,
}

impl<R: Read> GorillaDecoder<R> {
    pub fn new(reader: R) -> io::Result<Self> {
        let mut bit_reader = BitReader::new(reader);
        
        // 读取第一个时间戳
        let first_timestamp = bit_reader.read_bits(64)?;
        
        Ok(GorillaDecoder {
            bit_reader,
            first_timestamp,
            prev_timestamp: first_timestamp,
            prev_delta: 0,
            prev_value: 0.0,
            first_value: true,
        })
    }

    /// 解码下一个数据点
    pub fn decode(&mut self) -> io::Result<Option<(u64, f64)>> {
        // 如果是第一个点，读取值
        if self.first_value {
            let value_bits = match self.bit_reader.read_bits(64) {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                    return Err(e);
                }
            };
            
            let value = f64::from_bits(value_bits);
            self.prev_value = value;
            self.first_value = false;
            
            return Ok(Some((self.first_timestamp, value)));
        }

        // 解码时间戳 - delta-of-delta
        let delta_of_delta: i64;
        
        // 读取delta-of-delta编码
        match self.bit_reader.read_bit() {
            Ok(false) => {
                // 0前缀，delta_of_delta为0
                delta_of_delta = 0;
            }
            Ok(true) => {
                match self.bit_reader.read_bit() {
                    Ok(false) => {
                        // 10前缀，小范围变化
                        let bits = self.bit_reader.read_bits(7)? as i64;
                        // 7位有符号数，需要处理符号扩展
                        delta_of_delta = if (bits & 0x40) != 0 {
                            bits | !0x7F
                        } else {
                            bits
                        };
                    }
                    Ok(true) => {
                        match self.bit_reader.read_bit() {
                            Ok(false) => {
                                // 110前缀，中等范围变化
                                let bits = self.bit_reader.read_bits(9)? as i64;
                                // 9位有符号数，需要处理符号扩展
                                delta_of_delta = if (bits & 0x100) != 0 {
                                    bits | !0x1FF
                                } else {
                                    bits
                                };
                            }
                            Ok(true) => {
                                match self.bit_reader.read_bit() {
                                    Ok(false) => {
                                        // 1110前缀，较大范围变化
                                        let bits = self.bit_reader.read_bits(12)? as i64;
                                        // 12位有符号数，需要处理符号扩展
                                        delta_of_delta = if (bits & 0x800) != 0 {
                                            bits | !0xFFF
                                        } else {
                                            bits
                                        };
                                    }
                                    Ok(true) => {
                                        // 1111前缀，大范围变化
                                        let bits = self.bit_reader.read_bits(32)? as i64;
                                        // 32位有符号数，需要处理符号扩展
                                        delta_of_delta = if (bits & 0x80000000) != 0 {
                                            bits | !0xFFFFFFFF
                                        } else {
                                            bits
                                        };
                                    }
                                    Err(e) => {
                                        if e.kind() == io::ErrorKind::UnexpectedEof {
                                            return Ok(None);
                                        }
                                        return Err(e);
                                    }
                                }
                            }
                            Err(e) => {
                                if e.kind() == io::ErrorKind::UnexpectedEof {
                                    return Ok(None);
                                }
                                return Err(e);
                            }
                        }
                    }
                    Err(e) => {
                        if e.kind() == io::ErrorKind::UnexpectedEof {
                            return Ok(None);
                        }
                        return Err(e);
                    }
                }
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(e);
            }
        }

        // 计算实际时间戳
        let delta = self.prev_delta + delta_of_delta;
        let timestamp = (self.prev_timestamp as i64 + delta) as u64;
        
        self.prev_delta = delta;
        self.prev_timestamp = timestamp;

        // 解码浮点值 - XOR
        match self.bit_reader.read_bit() {
            Ok(false) => {
                // 值相同，直接返回
                return Ok(Some((timestamp, self.prev_value)));
            }
            Ok(true) => {
                // 值不同，读取XOR编码
                let leading_zeros = self.bit_reader.read_bits(5)? as u8;
                let significant_bits = self.bit_reader.read_bits(6)? as u8;
                
                let meaningful_bits = self.bit_reader.read_bits(significant_bits)?;
                let meaningful_bits_shifted = meaningful_bits << (64 - leading_zeros - significant_bits);
                
                let prev_value_bits = f64::to_bits(self.prev_value);
                let value_bits = prev_value_bits ^ meaningful_bits_shifted;
                let value = f64::from_bits(value_bits);
                
                self.prev_value = value;
                return Ok(Some((timestamp, value)));
            }
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(e);
            }
        }
    }

    /// 读取所有数据点
    pub fn decode_all(mut self) -> io::Result<Vec<(u64, f64)>> {
        let mut points = Vec::new();
        
        while let Some(point) = self.decode()? {
            points.push(point);
        }
        
        Ok(points)
    }
    
    /// 获取底层reader的引用
    pub fn get_ref(&self) -> &R {
        self.bit_reader.get_ref()
    }
    
    /// 获取底层reader的可变引用
    pub fn get_mut(&mut self) -> &mut R {
        self.bit_reader.get_mut()
    }
    
    /// 消耗解码器，返回底层reader
    pub fn into_inner(self) -> R {
        self.bit_reader.into_inner()
    }
}

/// 简单时序块，包含多个时序点(时间戳, 值)
pub struct TimeSeriesBlock {
    points: Vec<(u64, f64)>,
}

impl TimeSeriesBlock {
    pub fn new() -> Self {
        TimeSeriesBlock {
            points: Vec::new(),
        }
    }

    /// 添加一个数据点
    pub fn add_point(&mut self, timestamp: u64, value: f64) {
        self.points.push((timestamp, value));
    }

    /// 添加多个数据点
    pub fn add_points(&mut self, points: &[(u64, f64)]) {
        self.points.extend_from_slice(points);
    }
    
    /// 获取所有点
    pub fn get_points(&self) -> &[(u64, f64)] {
        &self.points
    }
    
    /// 获取点数量
    pub fn len(&self) -> usize {
        self.points.len()
    }
    
    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// 使用Gorilla算法压缩数据
    pub fn compress(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::new();
        
        // 按时间排序
        let mut sorted_points = self.points.clone();
        sorted_points.sort_by_key(|&(ts, _)| ts);
        
        // 写入数据块长度
        let len = sorted_points.len() as u32;
        buf.extend_from_slice(&len.to_le_bytes());
        
        // 压缩所有点
        let mut encoder = GorillaEncoder::new(&mut buf);
        for &(timestamp, value) in &sorted_points {
            encoder.encode(timestamp, value)?;
        }
        
        // 完成编码
        let mut writer = encoder.close()?;
        
        // 返回压缩后的数据
        Ok(buf)
    }

    /// 从Gorilla压缩数据中解压
    pub fn decompress(data: &[u8]) -> io::Result<Self> {
        // 读取数据块长度
        if data.len() < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Data too short to contain block length",
            ));
        }
        
        let len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let decoder = GorillaDecoder::new(&data[4..])?;
        
        // 解压所有点
        let points = decoder.decode_all()?;
        
        if points.len() != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected {} points, but got {}", len, points.len()),
            ));
        }
        
        Ok(TimeSeriesBlock { points })
    }
    
    /// 查询给定时间范围的数据点
    pub fn query(&self, start: u64, end: u64) -> Vec<(u64, f64)> {
        self.points
            .iter()
            .filter(|&&(ts, _)| ts >= start && ts <= end)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_gorilla_compression() {
        // 构造示例数据
        let mut block = TimeSeriesBlock::new();
        
        // 添加有规律的数据点
        for i in 0..100 {
            let ts = 1000 + i * 60;
            let val = 100.0 + (i as f64 % 10.0);
            block.add_point(ts, val);
        }
        
        // 压缩数据
        let compressed = block.compress().unwrap();
        
        // 计算压缩比率
        let original_size = block.len() * 16; // 每条记录16字节(8字节ts + 8字节value)
        let compressed_size = compressed.len();
        let ratio = compressed_size as f64 / original_size as f64;
        
        println!("压缩前大小: {} 字节", original_size);
        println!("压缩后大小: {} 字节", compressed_size);
        println!("压缩比率: {:.2}", ratio);
        
        // 解压数据
        let decompressed = TimeSeriesBlock::decompress(&compressed).unwrap();
        
        // 验证解压数据与原始数据一致
        assert_eq!(block.len(), decompressed.len());
        
        for (i, (&(orig_ts, orig_val), &(dec_ts, dec_val))) in 
                block.get_points().iter().zip(decompressed.get_points().iter()).enumerate() {
            assert_eq!(orig_ts, dec_ts, "时间戳不匹配在点 {}", i);
            assert_eq!(orig_val, dec_val, "数值不匹配在点 {}", i);
        }
    }
    
    #[test]
    fn test_query() {
        let mut block = TimeSeriesBlock::new();
        
        // 添加测试数据
        for i in 0..100 {
            block.add_point(1000 + i * 10, i as f64);
        }
        
        // 查询子范围
        let result = block.query(1200, 1400);
        
        // 验证结果
        assert_eq!(result.len(), 21); // 1200, 1210, ..., 1400
        assert_eq!(result[0].0, 1200);
        assert_eq!(result[0].1, 20.0);
        assert_eq!(result[20].0, 1400);
        assert_eq!(result[20].1, 40.0);
    }
}

