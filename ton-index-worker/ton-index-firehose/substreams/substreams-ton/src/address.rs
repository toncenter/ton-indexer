use std::fmt;
use std::str::FromStr;

use crate::pb::ton::state::v1 as state;
use crate::tvm::Error;

/// A standard account address: the workchain and the 32 bytes of the account id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Address {
    pub workchain: i32,
    pub account_id: [u8; 32],
}

impl Address {
    pub const fn new(workchain: i32, account_id: [u8; 32]) -> Self {
        Address { workchain, account_id }
    }

    pub fn from_pb(address: &state::Address) -> Result<Self, Error> {
        let account_id = address
            .account_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Protocol(format!("account_id of {} bytes", address.account_id.len())))?;
        Ok(Address::new(address.workchain, account_id))
    }

    pub fn to_pb(&self) -> state::Address {
        state::Address { workchain: self.workchain, account_id: self.account_id.to_vec(), ..Default::default() }
    }
}

/// The raw form `workchain:hex`, e.g. `-1:3333…3333`.
impl FromStr for Address {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (workchain, id) = s.split_once(':').ok_or_else(|| format!("{s:?} is not workchain:hex"))?;
        let workchain = workchain.parse().map_err(|_| format!("bad workchain in {s:?}"))?;
        let mut account_id = [0u8; 32];
        hex::decode_to_slice(id, &mut account_id).map_err(|_| format!("bad account id in {s:?}"))?;
        Ok(Address::new(workchain, account_id))
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.workchain, hex::encode_upper(self.account_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_form_round_trips() {
        let raw = "-1:3333333333333333333333333333333333333333333333333333333333333333";
        let address: Address = raw.parse().unwrap();
        assert_eq!(address.workchain, -1);
        assert_eq!(address.account_id, [0x33; 32]);
        assert_eq!(address.to_string(), raw);
        assert!("0:33".parse::<Address>().is_err());
    }
}
