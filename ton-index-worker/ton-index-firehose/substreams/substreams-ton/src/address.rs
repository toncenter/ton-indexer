//! Addresses: the [`StdAddr`] of rston, and `ton.state.v1.Address` of ton_v1,
//! which has standard addresses without anycast.

use rston::cell::HashBytes;
use rston::models::StdAddr;

use crate::pb::ton::state::v1 as state;
use crate::Error;

/// An address of an answer.
impl TryFrom<&state::Address> for StdAddr {
    type Error = Error;

    fn try_from(address: &state::Address) -> Result<Self, Error> {
        let workchain = i8::try_from(address.workchain)
            .map_err(|_| Error::Protocol(format!("workchain {} of a standard address", address.workchain)))?;
        let account_id = address
            .account_id
            .as_slice()
            .try_into()
            .map_err(|_| Error::Protocol(format!("account_id of {} bytes", address.account_id.len())))?;
        Ok(StdAddr::new(workchain, HashBytes(account_id)))
    }
}

/// The address of a call, or why ton_v1 cannot take it.
pub(crate) fn to_pb(address: &StdAddr) -> Result<state::Address, String> {
    if address.anycast.is_some() {
        return Err(format!("{address} has anycast: ton_v1 takes standard addresses without it"));
    }
    Ok(state::Address {
        workchain: address.workchain.into(),
        account_id: address.address.0.to_vec(),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_of_answers_convert() {
        let address = StdAddr::new(-1, HashBytes([0x33; 32]));
        let pb = to_pb(&address).unwrap();
        assert_eq!((pb.workchain, pb.account_id.as_slice()), (-1, &[0x33; 32][..]));
        assert_eq!(StdAddr::try_from(&pb).unwrap(), address);
        let short = state::Address { account_id: vec![0x33; 31], ..pb.clone() };
        assert!(matches!(StdAddr::try_from(&short), Err(Error::Protocol(_))));
        let wide = state::Address { workchain: 128, ..pb };
        assert!(matches!(StdAddr::try_from(&wide), Err(Error::Protocol(_))));
    }
}
