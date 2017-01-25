// Copyright 2015-2017 Aerospike, Inc.
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

use std::fmt;

#[derive(Debug,Clone,PartialEq)]
pub enum IndexType {
    Numeric,
    String,
    Geo2DSphere,
}

#[derive(Debug,Clone,PartialEq)]
pub enum CollectionIndexType {
    Default = 0,
    List,
    MapKeys,
    MapValues,
}

impl fmt::Display for IndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &IndexType::Numeric => "NUMERIC".fmt(f),
            &IndexType::String => "STRING".fmt(f),
            &IndexType::Geo2DSphere => "GEO2DSPHERE".fmt(f),
        }
    }
}

impl fmt::Display for CollectionIndexType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            &CollectionIndexType::Default => panic!("Unknown IndexCollectionType value `Default`"),
            &CollectionIndexType::List => try!("LIST".fmt(f)),
            &CollectionIndexType::MapKeys => try!("MAPKEYS".fmt(f)),
            &CollectionIndexType::MapValues => try!("MAPVALUES".fmt(f)),
        }
        Ok(())
    }
}