use crc::Algorithm;

pub const CRC_32_TARANTOOL: Algorithm<u32> = Algorithm {
    width: 32,
    poly: 0x1edc6f41,
    init: 0xffffffff,
    refin: true,
    refout: true,
    xorout: 0x0,
    check: 0xe3069283,
    residue: 0xb798b438,
};

pub fn calculate_bucket_id<T: AsRef<str>>(val: T, bucket_count: u32) -> u32 {
    let crc32 = crc::Crc::<u32>::new(&CRC_32_TARANTOOL);
    crc32.checksum(val.as_ref().as_bytes()) % bucket_count + 1
}
