use bitcoin::{Script, ScriptBuf};

pub type ScriptType = u8;
pub const COINBASE_OUTPUT_FLAG: u8 = 0x07;

pub trait ScriptBufExt {
    fn script_type(&self) -> ScriptType;
}

impl ScriptBufExt for ScriptBuf {
    #[rustfmt::skip]
    fn script_type(&self) -> ScriptType {
        if self.is_p2pk() { return 0x01; }
        if self.is_p2pkh() { return 0x02; }
        if self.is_p2sh() { return 0x03; }
        if self.is_p2wsh() { return 0x04; }
        if self.is_p2wpkh() { return 0x05; }
        if self.is_p2tr() { return 0x06; }
        0x00
    }
}

impl ScriptBufExt for Script {
    #[rustfmt::skip]
    fn script_type(&self) -> ScriptType {
        if self.is_p2pk() { return 0x01; }
        if self.is_p2pkh() { return 0x02; }
        if self.is_p2sh() { return 0x03; }
        if self.is_p2wsh() { return 0x04; }
        if self.is_p2wpkh() { return 0x05; }
        if self.is_p2tr() { return 0x06; }
        0x00
    }
}
