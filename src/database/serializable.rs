pub trait Serializable {
    fn from_u8(key: &[u8]) -> Self;
    fn as_u8(&self) -> Vec<u8>;
}

pub fn from_u8<K: Serializable>(key: &[u8]) -> K {
    Serializable::from_u8(key)
}

impl Serializable for i32 {
    fn from_u8(key: &[u8]) -> i32 {
        assert!(key.len() == 4);

        (key[0] as i32) << 24 | (key[1] as i32) << 16 | (key[2] as i32) << 8 | (key[3] as i32)
    }

    fn as_u8(&self) -> Vec<u8> {
        let mut dst = [0u8, 0, 0, 0];
        dst[0] = (*self >> 24) as u8;
        dst[1] = (*self >> 16) as u8;
        dst[2] = (*self >> 8) as u8;
        dst[3] = *self as u8;
        dst.to_vec()
    }
}
