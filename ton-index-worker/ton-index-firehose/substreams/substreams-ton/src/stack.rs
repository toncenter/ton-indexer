//! TVM stack values. A stack is given bottom to top: the first value a
//! get-method returns is at index 0.

use num_bigint::BigInt;

use crate::pb::ton::tvm::v1::{stack_entry::Value, StackEntry};
use crate::tvm::Error;

/// An integer: int257, minimal two's complement big-endian.
pub fn int(value: impl Into<BigInt>) -> StackEntry {
    StackEntry { value: Some(Value::Integer(value.into().to_signed_bytes_be())) }
}

/// A cell, by the BOC of one cell.
pub fn cell(boc: Vec<u8>) -> StackEntry {
    StackEntry { value: Some(Value::Cell(boc)) }
}

pub fn null() -> StackEntry {
    StackEntry { value: Some(Value::Null(())) }
}

/// Typed access to the values of a result stack.
pub trait StackExt {
    fn value(&self, index: usize) -> Result<&Value, Error>;

    fn int(&self, index: usize) -> Result<BigInt, Error> {
        match self.value(index)? {
            Value::Integer(bytes) if (1..=33).contains(&bytes.len()) => Ok(BigInt::from_signed_bytes_be(bytes)),
            Value::Integer(bytes) => Err(Error::Protocol(format!("integer of {} bytes", bytes.len()))),
            other => Err(type_error(index, "integer", other)),
        }
    }

    /// The BOC of the cell at index.
    fn cell(&self, index: usize) -> Result<&[u8], Error> {
        match self.value(index)? {
            Value::Cell(boc) => Ok(boc),
            other => Err(type_error(index, "cell", other)),
        }
    }
}

impl StackExt for [StackEntry] {
    fn value(&self, index: usize) -> Result<&Value, Error> {
        let entry = self
            .get(index)
            .ok_or_else(|| Error::Protocol(format!("stack of {} values has no index {index}", self.len())))?;
        entry.value.as_ref().ok_or_else(|| Error::Protocol(format!("stack value {index} is empty")))
    }
}

fn type_error(index: usize, expected: &str, got: &Value) -> Error {
    let got = match got {
        Value::Null(_) => "null",
        Value::Integer(_) => "integer",
        Value::Cell(_) => "cell",
        Value::Slice(_) => "slice",
        Value::Builder(_) => "builder",
        Value::Tuple(_) => "tuple",
        Value::Nan(_) => "nan",
        Value::Unsupported(name) => name.as_str(),
    };
    Error::Protocol(format!("stack value {index} is {got}, not {expected}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(value: impl Into<BigInt>) -> Vec<u8> {
        match int(value).value {
            Some(Value::Integer(bytes)) => bytes,
            _ => unreachable!(),
        }
    }

    #[test]
    fn integers_are_minimal_twos_complement() {
        assert_eq!(encoded(0), vec![0x00]);
        assert_eq!(encoded(127), vec![0x7f]);
        assert_eq!(encoded(128), vec![0x00, 0x80]);
        assert_eq!(encoded(-1), vec![0xff]);
        assert_eq!(encoded(-129), vec![0xff, 0x7f]);
        let one = BigInt::from(1);
        let max: BigInt = (&one << 256usize) - &one;
        assert_eq!(encoded(max.clone()).len(), 33);
        assert_eq!(vec![int(max.clone())].int(0).unwrap(), max);
        let min: BigInt = -(&one << 256usize);
        assert_eq!(vec![int(min.clone())].int(0).unwrap(), min);
    }

    #[test]
    fn wrong_types_are_errors() {
        let stack = vec![cell(vec![1]), null()];
        assert!(stack.int(0).is_err());
        assert_eq!(stack.cell(0).unwrap(), &[1]);
        assert!(stack.cell(2).is_err());
    }
}
