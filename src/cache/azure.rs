// Copyright 2018 Benjamin Bader
// Copyright 2016 Mozilla Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use opendal::Operator;

use opendal::layers::{HttpClientLayer, LoggingLayer};
use opendal::services::Azblob;

use crate::errors::*;

use super::http_client::set_user_agent;

pub struct AzureBlobCache;

/// Extract the blob endpoint from an Azure Storage connection string.
fn blob_endpoint_from_connection_string(conn: &str) -> Option<String> {
    for part in conn.split(';') {
        let part = part.trim();
        if let Some((key, value)) = part.split_once('=') {
            if key.eq_ignore_ascii_case("BlobEndpoint") {
                return Some(value.to_string());
            }
        }
    }
    None
}

impl AzureBlobCache {
    pub fn build(
        connection_string: &str,
        container: &str,
        key_prefix: &str,
        no_credentials: bool,
    ) -> Result<Operator> {
        if no_credentials {
            // Use the HTTP backend for anonymous (public) access so that
            // OpenDAL does not attempt credential discovery (e.g. IMDS).
            let endpoint = blob_endpoint_from_connection_string(connection_string)
                .ok_or_else(|| anyhow!("BlobEndpoint not found in connection string"))?;

            let root = format!("/{container}/{key_prefix}");
            let builder = opendal::services::Http::default()
                .endpoint(&endpoint)
                .root(&root);

            let op = Operator::new(builder)?
                .layer(HttpClientLayer::new(set_user_agent()))
                .layer(LoggingLayer::default())
                .finish();
            Ok(op)
        } else {
            let builder = Azblob::from_connection_string(connection_string)?
                .container(container)
                .root(key_prefix);

            let op = Operator::new(builder)?
                .layer(HttpClientLayer::new(set_user_agent()))
                .layer(LoggingLayer::default())
                .finish();
            Ok(op)
        }
    }
}
