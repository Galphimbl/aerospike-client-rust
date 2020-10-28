// Copyright 2015-2020 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

//! Functions used for Filter Expressions. This module requires Aerospike Server version >= 5.2

pub mod bitwise;
pub mod hll;
pub mod lists;
pub mod maps;
pub mod regex_flag;

use crate::commands::buffer::Buffer;
use crate::errors::Result;
use crate::msgpack::encoder::{pack_array_begin, pack_integer, pack_raw_string, pack_value};
use crate::operations::cdt_context::CdtContext;
use crate::{ParticleType, Value};
use std::collections::HashMap;
use std::fmt::Debug;

/// Expression Data Types for usage in some `FilterExpressions` on for example Map and List
#[derive(Debug, Clone, Copy)]
pub enum ExpType {
    /// NIL Expression Type
    NIL = 0,
    /// BOOLEAN Expression Type
    BOOL = 1,
    /// INTEGER Expression Type
    INT = 2,
    /// STRING Expression Type
    STRING = 3,
    /// LIST Expression Type
    LIST = 4,
    /// MAP Expression Type
    MAP = 5,
    /// BLOB Expression Type
    BLOB = 6,
    /// FLOAT Expression Type
    FLOAT = 7,
    /// GEO String Expression Type
    GEO = 8,
    /// HLL Expression Type
    HLL = 9,
}

#[derive(Debug, Clone, Copy)]
#[doc(hidden)]
pub enum ExpOp {
    EQ = 1,
    NE = 2,
    GT = 3,
    GE = 4,
    LT = 5,
    LE = 6,
    Regex = 7,
    Geo = 8,
    And = 16,
    Or = 17,
    Not = 18,
    DigestModulo = 64,
    DeviceSize = 65,
    LastUpdate = 66,
    SinceUpdate = 67,
    VoidTime = 68,
    TTL = 69,
    SetName = 70,
    KeyExists = 71,
    IsTombstone = 72,
    Key = 80,
    Bin = 81,
    BinType = 82,
    Quoted = 126,
    Call = 127,
}

#[doc(hidden)]
pub const MODIFY: i64 = 0x40;

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum ExpressionArgument {
    Value(Value),
    FilterExpression(FilterExpression),
    Context(Vec<CdtContext>),
}

/// Filter expression, which can be applied to most commands, to control which records are
/// affected by the command. Filter expression are created using the functions in the
/// [expressions](crate::expressions) module and its submodules.
#[derive(Debug, Clone)]
pub struct FilterExpression {
    /// The Operation code
    cmd: Option<ExpOp>,
    /// The Primary Value of the Operation
    val: Option<Value>,
    /// The Bin to use it on (REGEX for example)
    bin: Option<Box<FilterExpression>>,
    /// The additional flags for the Operation (REGEX or return_type of Module for example)
    flags: Option<i64>,
    /// The optional Module flag for Module operations or Bin Types
    module: Option<ExpType>,
    /// Sub commands for the CmdExp operation
    exps: Option<Vec<FilterExpression>>,

    arguments: Option<Vec<ExpressionArgument>>,
}

#[doc(hidden)]
impl FilterExpression {
    fn new(
        cmd: Option<ExpOp>,
        val: Option<Value>,
        bin: Option<FilterExpression>,
        flags: Option<i64>,
        module: Option<ExpType>,
        exps: Option<Vec<FilterExpression>>,
    ) -> FilterExpression {
        if let Some(bin) = bin {
            FilterExpression {
                cmd,
                val,
                bin: Some(Box::new(bin)),
                flags,
                module,
                exps,
                arguments: None,
            }
        } else {
            FilterExpression {
                cmd,
                val,
                bin: None,
                flags,
                module,
                exps,
                arguments: None,
            }
        }
    }

    fn pack_expression(
        &self,
        exps: &[FilterExpression],
        buf: &mut Option<&mut Buffer>,
    ) -> Result<usize> {
        let mut size = 0;
        size += pack_array_begin(buf, exps.len() + 1)?;
        size += pack_integer(buf, self.cmd.unwrap() as i64)?;
        for exp in exps {
            size += exp.pack(buf)?;
        }
        Ok(size)
    }

    fn pack_command(&self, cmd: ExpOp, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;

        match cmd {
            ExpOp::Regex => {
                size += pack_array_begin(buf, 4)?;
                // The Operation
                size += pack_integer(buf, cmd as i64)?;
                // Regex Flags
                size += pack_integer(buf, self.flags.unwrap())?;
                // Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
                // The Bin
                size += self.bin.clone().unwrap().pack(buf)?;
            }
            ExpOp::Call => {
                // Packing logic for Module
                size += pack_array_begin(buf, 5)?;
                // The Operation
                size += pack_integer(buf, cmd as i64)?;
                // The Module Operation
                size += pack_integer(buf, self.module.unwrap() as i64)?;
                // The Module (List/Map or Bitwise)
                size += pack_integer(buf, self.flags.unwrap())?;
                // Encoding the Arguments
                if let Some(args) = &self.arguments {
                    let mut len = 0;
                    for arg in args {
                        // First match to estimate the Size and write the Context
                        match arg {
                            ExpressionArgument::Value(_)
                            | ExpressionArgument::FilterExpression(_) => len += 1,
                            ExpressionArgument::Context(ctx) => {
                                if !ctx.is_empty() {
                                    pack_array_begin(buf, 3)?;
                                    pack_integer(buf, 0xff)?;
                                    pack_array_begin(buf, ctx.len() * 2)?;

                                    for c in ctx {
                                        pack_integer(buf, i64::from(c.id))?;
                                        pack_value(buf, &c.value)?;
                                    }
                                }
                            }
                        }
                    }
                    size += pack_array_begin(buf, len)?;
                    // Second match to write the real values
                    for arg in args {
                        match arg {
                            ExpressionArgument::Value(val) => {
                                size += pack_value(buf, val)?;
                            }
                            ExpressionArgument::FilterExpression(cmd) => {
                                size += cmd.pack(buf)?;
                            }
                            _ => {}
                        }
                    }
                } else {
                    // No Arguments
                    size += pack_value(buf, &self.val.clone().unwrap())?;
                }
                // Write the Bin
                size += self.bin.clone().unwrap().pack(buf)?;
            }
            ExpOp::Bin => {
                // Bin Encoder
                size += pack_array_begin(buf, 3)?;
                // The Bin Operation
                size += pack_integer(buf, cmd as i64)?;
                // The Bin Type (INT/String etc.)
                size += pack_integer(buf, self.module.unwrap() as i64)?;
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
            }
            ExpOp::BinType => {
                // BinType encoder
                size += pack_array_begin(buf, 2)?;
                // BinType Operation
                size += pack_integer(buf, cmd as i64)?;
                // The name - Raw String is needed instead of the msgpack String that the pack_value method would use.
                size += pack_raw_string(buf, &self.val.clone().unwrap().to_string())?;
            }
            _ => {
                // Packing logic for all other Ops
                if let Some(value) = &self.val {
                    // Operation has a Value
                    size += pack_array_begin(buf, 2)?;
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64)?;
                    // Write the Value
                    size += pack_value(buf, value)?;
                } else {
                    // Operation has no Value
                    size += pack_array_begin(buf, 1)?;
                    // Write the Operation
                    size += pack_integer(buf, cmd as i64)?;
                }
            }
        }

        Ok(size)
    }

    fn pack_value(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        // Packing logic for Value based Ops
        pack_value(buf, &self.val.clone().unwrap())
    }

    pub fn pack(&self, buf: &mut Option<&mut Buffer>) -> Result<usize> {
        let mut size = 0;
        if let Some(exps) = &self.exps {
            size += self.pack_expression(exps, buf)?;
        } else if let Some(cmd) = self.cmd {
            size += self.pack_command(cmd, buf)?;
        } else {
            size += self.pack_value(buf)?;
        }

        Ok(size)
    }
}

/// Create a record key expression of specified type.
/// ```
/// use aerospike::expressions::{ExpType, ge, int_val, key};
/// // Integer record key >= 100000
/// ge(key(ExpType::INT), int_val(10000));
/// ```
pub fn key(exp_type: ExpType) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Key),
        Some(Value::from(exp_type as i64)),
        None,
        None,
        None,
        None,
    )
}

/// Create function that returns if the primary key is stored in the record meta data
/// as a boolean expression. This would occur when `send_key` is true on record write.
/// ```
/// // Key exists in record meta data
/// use aerospike::expressions::key_exists;
/// key_exists();
/// ```
pub fn key_exists() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::KeyExists), None, None, None, None, None)
}

/// Create 64 bit int bin expression.
/// ```
/// // Integer bin "a" == 500
/// use aerospike::expressions::{int_bin, int_val, eq};
/// eq(int_bin("a".to_string()), int_val(500));
/// ```
pub fn int_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::INT),
        None,
    )
}

/// Create string bin expression.
/// ```
/// // String bin "a" == "views"
/// use aerospike::expressions::{eq, string_bin, string_val};
/// eq(string_bin("a".to_string()), string_val("views".to_string()));
/// ```
pub fn string_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::STRING),
        None,
    )
}

/// Create blob bin expression.
/// ```
/// // String bin "a" == [1,2,3]
/// use aerospike::expressions::{eq, blob_bin, blob_val};
/// let blob: Vec<u8> = vec![1,2,3];
/// eq(blob_bin("a".to_string()), blob_val(blob));
/// ```
pub fn blob_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::BLOB),
        None,
    )
}

/// Create 64 bit float bin expression.
/// ```
/// use aerospike::expressions::{float_val, float_bin, eq};
/// // Integer bin "a" == 500.5
/// eq(float_bin("a".to_string()), float_val(500.5));
/// ```
pub fn float_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::FLOAT),
        None,
    )
}

/// Create geo bin expression.
/// ```
/// // String bin "a" == region
/// use aerospike::expressions::{eq, geo_bin, string_val};
/// let region = "{ \"type\": \"AeroCircle\", \"coordinates\": [[-122.0, 37.5], 50000.0] }";
/// eq(geo_bin("a".to_string()), string_val(region.to_string()));
/// ```
pub fn geo_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::GEO),
        None,
    )
}

/// Create list bin expression.
/// ```
/// use aerospike::expressions::{ExpType, eq, int_val, list_bin};
/// use aerospike::operations::lists::ListReturnType;
/// use aerospike::expressions::lists::get_by_index;
/// // String bin a[2] == 3
/// eq(get_by_index(ListReturnType::Values, ExpType::INT, int_val(2), list_bin("a".to_string()), &[]), int_val(3));
/// ```
pub fn list_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::LIST),
        None,
    )
}

/// Create map bin expression.
///
/// ```
/// // Bin a["key"] == "value"
/// use aerospike::expressions::{ExpType, string_val, map_bin, eq};
/// use aerospike::MapReturnType;
/// use aerospike::expressions::maps::get_by_key;
///
/// eq(
///     get_by_key(MapReturnType::Value, ExpType::STRING, string_val("key".to_string()), map_bin("a".to_string()), &[]),
///     string_val("value".to_string()));
/// ```
pub fn map_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::MAP),
        None,
    )
}

/// Create a HLL bin expression
///
/// ```
/// use aerospike::expressions::{gt, list_val, hll_bin, int_val};
/// use aerospike::operations::hll::HLLPolicy;
/// use aerospike::Value;
/// use aerospike::expressions::hll::add;
///
/// // Add values to HLL bin "a" and check count > 7
/// let list = vec![Value::from(1)];
/// gt(add(HLLPolicy::default(), list_val(list), hll_bin("a".to_string())), int_val(7));
/// ```
pub fn hll_bin(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Bin),
        Some(Value::from(name)),
        None,
        None,
        Some(ExpType::HLL),
        None,
    )
}

/// Create function that returns if bin of specified name exists.
/// ```
/// // Bin "a" exists in record
/// use aerospike::expressions::bin_exists;
/// bin_exists("a".to_string());
/// ```
pub fn bin_exists(name: String) -> FilterExpression {
    ne(bin_type(name), int_val(ParticleType::NULL as i64))
}

/// Create function that returns bin's integer particle type.
/// ```
/// use aerospike::ParticleType;
/// use aerospike::expressions::{eq, bin_type, int_val};
/// // Bin "a" particle type is a list
/// eq(bin_type("a".to_string()), int_val(ParticleType::LIST as i64));
/// ```
pub fn bin_type(name: String) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::BinType),
        Some(Value::from(name)),
        None,
        None,
        None,
        None,
    )
}

/// Create function that returns record set name string.
/// ```
/// use aerospike::expressions::{eq, set_name, string_val};
/// // Record set name == "myset
/// eq(set_name(), string_val("myset".to_string()));
/// ```
pub fn set_name() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::SetName), None, None, None, None, None)
}

/// Create function that returns record size on disk.
/// If server storage-engine is memory, then zero is returned.
/// ```
/// use aerospike::expressions::{ge, device_size, int_val};
/// // Record device size >= 100 KB
/// ge(device_size(), int_val(100*1024));
/// ```
pub fn device_size() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::DeviceSize), None, None, None, None, None)
}

/// Create function that returns record last update time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// // Record last update time >=2020-08-01
/// use aerospike::expressions::{ge, last_update, float_val};
/// ge(last_update(), float_val(1.5962E+18));
/// ```
pub fn last_update() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::LastUpdate), None, None, None, None, None)
}

/// Create expression that returns milliseconds since the record was last updated.
/// This expression usually evaluates quickly because record meta data is cached in memory.
///
/// ```
/// // Record last updated more than 2 hours ago
/// use aerospike::expressions::{gt, int_val, since_update};
/// gt(since_update(), int_val(2 * 60 * 60 * 1000));
/// ```
pub fn since_update() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::SinceUpdate), None, None, None, None, None)
}

/// Create function that returns record expiration time expressed as 64 bit integer
/// nanoseconds since 1970-01-01 epoch.
/// ```
/// // Expires on 2020-08-01
/// use aerospike::expressions::{and, ge, last_update, float_val, lt};
/// and(vec![ge(last_update(), float_val(1.5962E+18)), lt(last_update(), float_val(1.5963E+18))]);
/// ```
pub fn void_time() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::VoidTime), None, None, None, None, None)
}

/// Create function that returns record expiration time (time to live) in integer seconds.
/// ```
/// // Record expires in less than 1 hour
/// use aerospike::expressions::{lt, ttl, int_val};
/// lt(ttl(), int_val(60*60));
/// ```
pub fn ttl() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::TTL), None, None, None, None, None)
}

/// Create expression that returns if record has been deleted and is still in tombstone state.
/// This expression usually evaluates quickly because record meta data is cached in memory.
///
/// ```
/// // Deleted records that are in tombstone state.
/// use aerospike::expressions::{is_tombstone};
/// is_tombstone();
/// ```
pub fn is_tombstone() -> FilterExpression {
    FilterExpression::new(Some(ExpOp::IsTombstone), None, None, None, None, None)
}
/// Create function that returns record digest modulo as integer.
/// ```
/// // Records that have digest(key) % 3 == 1
/// use aerospike::expressions::{int_val, eq, digest_modulo};
/// eq(digest_modulo(3), int_val(1));
/// ```
pub fn digest_modulo(modulo: i64) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::DigestModulo),
        Some(Value::from(modulo)),
        None,
        None,
        None,
        None,
    )
}

/// Create function like regular expression string operation.
/// ```
/// use aerospike::RegexFlag;
/// use aerospike::expressions::{regex_compare, string_bin};
/// // Select string bin "a" that starts with "prefix" and ends with "suffix".
/// // Ignore case and do not match newline.
/// regex_compare("prefix.*suffix".to_string(), RegexFlag::ICASE as i64 | RegexFlag::NEWLINE as i64, string_bin("a".to_string()));
/// ```
pub fn regex_compare(regex: String, flags: i64, bin: FilterExpression) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Regex),
        Some(Value::from(regex)),
        Some(bin),
        Some(flags),
        None,
        None,
    )
}

/// Create compare geospatial operation.
/// ```
/// use aerospike::expressions::{geo_compare, geo_bin, geo_val};
/// // Query region within coordinates.
/// let region = "{\"type\": \"Polygon\", \"coordinates\": [ [[-122.500000, 37.000000],[-121.000000, 37.000000], [-121.000000, 38.080000],[-122.500000, 38.080000], [-122.500000, 37.000000]] ] }";
/// geo_compare(geo_bin("a".to_string()), geo_val(region.to_string()));
/// ```
pub fn geo_compare(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Geo),
        None,
        None,
        None,
        None,
        Some(vec![left, right]),
    )
}

/// Creates 64 bit integer value
pub fn int_val(val: i64) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates a Boolean value
pub fn bool_val(val: bool) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates String bin value
pub fn string_val(val: String) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates 64 bit float bin value
pub fn float_val(val: f64) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Creates Blob bin value
pub fn blob_val(val: Vec<u8>) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Create List bin Value
pub fn list_val(val: Vec<Value>) -> FilterExpression {
    FilterExpression::new(
        Some(ExpOp::Quoted),
        Some(Value::from(val)),
        None,
        None,
        None,
        None,
    )
}

/// Create Map bin Value
pub fn map_val(val: HashMap<Value, Value>) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Create geospatial json string value.
pub fn geo_val(val: String) -> FilterExpression {
    FilterExpression::new(None, Some(Value::from(val)), None, None, None, None)
}

/// Create a Nil Value
pub fn nil() -> FilterExpression {
    FilterExpression::new(None, Some(Value::Nil), None, None, None, None)
}
/// Create "not" operator expression.
/// ```
/// // ! (a == 0 || a == 10)
/// use aerospike::expressions::{not, or, eq, int_bin, int_val};
/// not(or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("a".to_string()), int_val(10))]));
/// ```
pub fn not(exp: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Not),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![exp]),
        arguments: None,
    }
}

/// Create "and" (&&) operator that applies to a variable number of expressions.
/// ```
/// // (a > 5 || a == 0) && b < 3
/// use aerospike::expressions::{and, or, gt, int_bin, int_val, eq, lt};
/// and(vec![or(vec![gt(int_bin("a".to_string()), int_val(5)), eq(int_bin("a".to_string()), int_val(0))]), lt(int_bin("b".to_string()), int_val(3))]);
/// ```
pub fn and(exps: Vec<FilterExpression>) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::And),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
    }
}

/// Create "or" (||) operator that applies to a variable number of expressions.
/// ```
/// // a == 0 || b == 0
/// use aerospike::expressions::{or, eq, int_bin, int_val};
/// or(vec![eq(int_bin("a".to_string()), int_val(0)), eq(int_bin("b".to_string()), int_val(0))]);
/// ```
pub fn or(exps: Vec<FilterExpression>) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::Or),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(exps),
        arguments: None,
    }
}

/// Create equal (==) expression.
/// ```
/// // a == 11
/// use aerospike::expressions::{eq, int_bin, int_val};
/// eq(int_bin("a".to_string()), int_val(11));
/// ```
pub fn eq(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::EQ),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

/// Create not equal (!=) expression
/// ```
/// // a != 13
/// use aerospike::expressions::{ne, int_bin, int_val};
/// ne(int_bin("a".to_string()), int_val(13));
/// ```
pub fn ne(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::NE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

/// Create greater than (>) operation.
/// ```
/// // a > 8
/// use aerospike::expressions::{gt, int_bin, int_val};
/// gt(int_bin("a".to_string()), int_val(8));
/// ```
pub fn gt(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::GT),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

/// Create greater than or equal (>=) operation.
/// ```
/// use aerospike::expressions::{ge, int_bin, int_val};
/// // a >= 88
/// ge(int_bin("a".to_string()), int_val(88));
/// ```
pub fn ge(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::GE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

/// Create less than (<) operation.
/// ```
/// // a < 1000
/// use aerospike::expressions::{lt, int_bin, int_val};
/// lt(int_bin("a".to_string()), int_val(1000));
/// ```
pub fn lt(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::LT),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

/// Create less than or equals (<=) operation.
/// ```
/// use aerospike::expressions::{le, int_bin, int_val};
/// // a <= 1
/// le(int_bin("a".to_string()), int_val(1));
/// ```
pub fn le(left: FilterExpression, right: FilterExpression) -> FilterExpression {
    FilterExpression {
        cmd: Some(ExpOp::LE),
        val: None,
        bin: None,
        flags: None,
        module: None,
        exps: Some(vec![left, right]),
        arguments: None,
    }
}

// ----------------------------------------------
