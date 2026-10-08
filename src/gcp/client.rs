// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::client::builder::HttpRequestBuilder;
use crate::client::get::GetClient;
use crate::client::header::{HeaderConfig, get_put_result, get_version};
use crate::client::list::ListClient;
use crate::client::retry::{RetryContext, RetryExt};
use crate::client::s3::{
    CompleteMultipartUpload, CompleteMultipartUploadResult, InitiateMultipartUploadResult,
    ListResponse, to_list_result,
};
use crate::client::{GetOptionsExt, HttpClient, HttpError, HttpResponse};
use crate::gcp::credential::CredentialExt;
use crate::gcp::{GcpCredential, GcpCredentialProvider, GcpSigningCredentialProvider, STORE};
use crate::list::{PaginatedListOptions, PaginatedListResult};
use crate::multipart::PartId;
use crate::path::Path;
use crate::util::hex_encode;
use crate::{
    Attribute, Attributes, ClientOptions, CopyMode, GetOptions, MultipartId, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, RetryConfig,
};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Buf;
use futures_util::StreamExt;
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_TYPE,
};
use http::{HeaderName, Method, StatusCode};
use md5::{Digest, Md5};
use percent_encoding::{NON_ALPHANUMERIC, percent_encode, utf8_percent_encode};
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const VERSION_HEADER: &str = "x-goog-generation";
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const USER_DEFINED_METADATA_HEADER_PREFIX: &str = "x-goog-meta-";
const STORAGE_CLASS: &str = "x-goog-storage-class";

static VERSION_MATCH: HeaderName = HeaderName::from_static("x-goog-if-generation-match");

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Error performing list request: {}", source)]
    ListRequest {
        source: crate::client::retry::RetryError,
        /// The prefix that was being listed, used only for error classification
        path: String,
    },

    #[error("Error getting list response body: {}", source)]
    ListResponseBody { source: HttpError },

    #[error("Got invalid list response: {}", source)]
    InvalidListResponse { source: quick_xml::de::DeError },

    #[error("Error performing get request {}: {}", path, source)]
    GetRequest {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[error("Error performing request {}: {}", path, source)]
    Request {
        source: crate::client::retry::RetryError,
        path: String,
    },

    #[error("Error getting put response body: {}", source)]
    PutResponseBody { source: HttpError },

    #[error("Got invalid put request: {}", source)]
    InvalidPutRequest { source: quick_xml::se::SeError },

    #[error("Got invalid put response: {}", source)]
    InvalidPutResponse { source: quick_xml::de::DeError },

    #[error("Unable to extract metadata from headers: {}", source)]
    Metadata {
        source: crate::client::header::Error,
    },

    #[error("Version required for conditional update")]
    MissingVersion,

    #[error("Error performing complete multipart request: {}", source)]
    CompleteMultipartRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Error getting complete multipart response body: {}", source)]
    CompleteMultipartResponseBody { source: HttpError },

    #[error("Got invalid multipart response: {}", source)]
    InvalidMultipartResponse { source: quick_xml::de::DeError },

    #[error("Error performing DeleteObjects request: {}", source)]
    DeleteObjectsRequest {
        source: crate::client::retry::RetryError,
        paths: Vec<String>,
    },

    #[error(
        "DeleteObjects request failed for key {}: {} (code: {})",
        path,
        message,
        code
    )]
    DeleteFailed {
        path: String,
        code: String,
        message: String,
    },

    #[error("Error getting DeleteObjects response body: {}", source)]
    DeleteObjectsResponse { source: HttpError },

    #[error("Got invalid DeleteObjects response: {}", source)]
    InvalidDeleteObjectsResponse {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    #[error("Error signing blob: {}", source)]
    SignBlobRequest {
        source: crate::client::retry::RetryError,
    },

    #[error("Got invalid signing blob response: {}", source)]
    InvalidSignBlobResponse { source: HttpError },

    #[error("Got invalid signing blob signature: {}", source)]
    InvalidSignBlobSignature { source: base64::DecodeError },
}

impl From<DeleteError> for Error {
    fn from(err: DeleteError) -> Self {
        Self::DeleteFailed {
            path: err.key,
            code: err.code,
            message: err.message,
        }
    }
}

impl From<Error> for crate::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::Request { source, path }
            | Error::ListRequest { source, path } => source.error(STORE, path),
            Error::DeleteObjectsRequest { source, paths } => source.error(STORE, paths.join(",")),
            _ => Self::Generic {
                store: STORE,
                source: Box::new(err),
            },
        }
    }
}

/// Builds the XML body of a multi-object delete request for `paths`.
fn delete_objects_body(paths: &[Path]) -> std::io::Result<Vec<u8>> {
    let mut writer = quick_xml::Writer::new(Vec::new());
    writer.write_event(Event::Start(
        BytesStart::new("Delete")
            .with_attributes([("xmlns", "http://s3.amazonaws.com/doc/2006-03-01/")]),
    ))?;
    for path in paths {
        writer.write_event(Event::Start(BytesStart::new("Object")))?;
        writer.write_event(Event::Start(BytesStart::new("Key")))?;
        writer.write_event(Event::Text(BytesText::new(path.as_ref())))?;
        writer.write_event(Event::End(BytesEnd::new("Key")))?;
        writer.write_event(Event::End(BytesEnd::new("Object")))?;
    }
    writer.write_event(Event::End(BytesEnd::new("Delete")))?;
    Ok(writer.into_inner())
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "DeleteResult")]
struct BatchDeleteResponse {
    #[serde(rename = "$value", default)]
    content: Vec<DeleteObjectResult>,
}

#[derive(Deserialize)]
enum DeleteObjectResult {
    #[allow(unused)]
    Deleted(DeletedObject),
    Error(DeleteError),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Deleted")]
struct DeletedObject {
    #[allow(dead_code)]
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", rename = "Error")]
struct DeleteError {
    key: String,
    code: String,
    message: String,
}

/// Error returned when a multi-object delete response names a key that wasn't requested.
#[derive(Debug, thiserror::Error)]
#[error("DeleteObjects response contains unexpected key {key}")]
struct UnexpectedDeleteKey {
    key: String,
}

impl BatchDeleteResponse {
    /// Returns one result per entry in `paths`, in the same order.
    ///
    /// Keys absent from the response are treated as deleted, which covers responses to
    /// requests with `Quiet` set.
    fn into_results(
        self,
        paths: &[Path],
    ) -> Result<Vec<Result<Path, DeleteError>>, UnexpectedDeleteKey> {
        let mut results: Vec<Result<Path, DeleteError>> = paths.iter().cloned().map(Ok).collect();
        for content in self.content {
            if let DeleteObjectResult::Error(error) = content {
                let i = paths
                    .iter()
                    .position(|p| p.as_ref() == error.key)
                    .ok_or_else(|| UnexpectedDeleteKey {
                        key: error.key.clone(),
                    })?;
                results[i] = Err(error);
            }
        }
        Ok(results)
    }
}

#[derive(Debug)]
pub(crate) struct GoogleCloudStorageConfig {
    pub base_url: String,

    pub credentials: GcpCredentialProvider,

    pub signing_credentials: GcpSigningCredentialProvider,

    pub bucket_name: String,

    pub retry_config: RetryConfig,

    pub client_options: ClientOptions,

    pub skip_signature: bool,
}

impl GoogleCloudStorageConfig {
    pub(crate) fn path_url(&self, path: &Path) -> String {
        format!("{}/{}/{}", self.base_url, self.bucket_name, path)
    }

    pub(crate) async fn get_credential(&self) -> Result<Option<Arc<GcpCredential>>> {
        Ok(match self.skip_signature {
            false => Some(self.credentials.get_credential().await?),
            true => None,
        })
    }
}

/// A builder for a put request allowing customisation of the headers and query string
pub(crate) struct Request<'a> {
    path: &'a Path,
    config: &'a GoogleCloudStorageConfig,
    payload: Option<PutPayload>,
    builder: HttpRequestBuilder,
    idempotent: bool,
}

impl Request<'_> {
    fn header(self, k: &HeaderName, v: &str) -> Self {
        let builder = self.builder.header(k, v);
        Self { builder, ..self }
    }

    fn query<T: Serialize + ?Sized + Sync>(self, query: &T) -> Self {
        let builder = self.builder.query(query);
        Self { builder, ..self }
    }

    fn idempotent(mut self, idempotent: bool) -> Self {
        self.idempotent = idempotent;
        self
    }

    fn with_attributes(self, attributes: Attributes) -> Self {
        let mut builder = self.builder;
        let mut has_content_type = false;
        for (k, v) in &attributes {
            builder = match k {
                Attribute::CacheControl => builder.header(CACHE_CONTROL, v.as_ref()),
                Attribute::ContentDisposition => builder.header(CONTENT_DISPOSITION, v.as_ref()),
                Attribute::ContentEncoding => builder.header(CONTENT_ENCODING, v.as_ref()),
                Attribute::ContentLanguage => builder.header(CONTENT_LANGUAGE, v.as_ref()),
                Attribute::ContentType => {
                    has_content_type = true;
                    builder.header(CONTENT_TYPE, v.as_ref())
                }
                Attribute::StorageClass => builder.header(STORAGE_CLASS, v.as_ref()),
                Attribute::Metadata(k_suffix) => builder.header(
                    &format!("{USER_DEFINED_METADATA_HEADER_PREFIX}{k_suffix}"),
                    v.as_ref(),
                ),
            };
        }

        if !has_content_type {
            let value = self.config.client_options.get_content_type(self.path);
            builder = builder.header(CONTENT_TYPE, value.unwrap_or(DEFAULT_CONTENT_TYPE))
        }
        Self { builder, ..self }
    }

    fn with_payload(self, payload: PutPayload) -> Self {
        let content_length = payload.content_length();
        Self {
            builder: self.builder.header(CONTENT_LENGTH, content_length),
            payload: Some(payload),
            ..self
        }
    }

    fn with_extensions(self, extensions: ::http::Extensions) -> Self {
        let builder = self.builder.extensions(extensions);
        Self { builder, ..self }
    }

    async fn send(self) -> Result<HttpResponse> {
        let credential = self.config.credentials.get_credential().await?;
        let resp = self
            .builder
            .bearer_auth(&credential.bearer)
            .retryable(&self.config.retry_config)
            .idempotent(self.idempotent)
            .payload(self.payload)
            .send()
            .await
            .map_err(|source| {
                let path = self.path.as_ref().into();
                Error::Request { source, path }
            })?;
        Ok(resp)
    }

    async fn do_put(self) -> Result<PutResult> {
        let response = self.send().await?;
        Ok(get_put_result(response.headers(), VERSION_HEADER)
            .map_err(|source| Error::Metadata { source })?)
    }
}

/// Sign Blob Request Body
#[derive(Debug, Serialize)]
struct SignBlobBody {
    /// The payload to sign
    payload: String,
}

/// Sign Blob Response
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignBlobResponse {
    /// The signature for the payload
    signed_blob: String,
}

#[derive(Debug)]
pub(crate) struct GoogleCloudStorageClient {
    config: GoogleCloudStorageConfig,

    client: HttpClient,

    bucket_name_encoded: String,

    // TODO: Hook this up in tests
    max_list_results: Option<String>,
}

impl GoogleCloudStorageClient {
    pub(crate) fn new(config: GoogleCloudStorageConfig, client: HttpClient) -> Result<Self> {
        let bucket_name_encoded =
            percent_encode(config.bucket_name.as_bytes(), NON_ALPHANUMERIC).to_string();

        Ok(Self {
            config,
            client,
            bucket_name_encoded,
            max_list_results: None,
        })
    }

    pub(crate) fn config(&self) -> &GoogleCloudStorageConfig {
        &self.config
    }

    async fn get_credential(&self) -> Result<Option<Arc<GcpCredential>>> {
        self.config.get_credential().await
    }

    /// Create a signature from a string-to-sign using Google Cloud signBlob method.
    /// form like:
    /// ```plaintext
    /// curl -X POST --data-binary @JSON_FILE_NAME \
    /// -H "Authorization: Bearer OAUTH2_TOKEN" \
    /// -H "Content-Type: application/json" \
    /// "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/SERVICE_ACCOUNT_EMAIL:signBlob"
    /// ```
    ///
    /// 'JSON_FILE_NAME' is a file containing the following JSON object:
    /// ```plaintext
    /// {
    ///  "payload": "REQUEST_INFORMATION"
    /// }
    /// ```
    pub(crate) async fn sign_blob(
        &self,
        string_to_sign: &str,
        client_email: &str,
    ) -> Result<String> {
        let credential = self.get_credential().await?;
        let body = SignBlobBody {
            payload: BASE64_STANDARD.encode(string_to_sign),
        };

        let url = format!(
            "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{client_email}:signBlob"
        );

        let response = self
            .client
            .post(&url)
            .with_bearer_auth(credential.as_deref())
            .json(&body)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::SignBlobRequest { source })?
            .into_body()
            .json::<SignBlobResponse>()
            .await
            .map_err(|source| Error::InvalidSignBlobResponse { source })?;

        let signed_blob = BASE64_STANDARD
            .decode(response.signed_blob)
            .map_err(|source| Error::InvalidSignBlobSignature { source })?;

        Ok(hex_encode(&signed_blob))
    }

    pub(crate) fn object_url(&self, path: &Path) -> String {
        let encoded = utf8_percent_encode(path.as_ref(), NON_ALPHANUMERIC);
        format!(
            "{}/{}/{}",
            self.config.base_url, self.bucket_name_encoded, encoded
        )
    }

    /// Perform a put request <https://cloud.google.com/storage/docs/xml-api/put-object-upload>
    ///
    /// Returns the new ETag
    pub(crate) fn request<'a>(&'a self, method: Method, path: &'a Path) -> Request<'a> {
        let builder = self.client.request(method, self.object_url(path));

        Request {
            path,
            builder,
            payload: None,
            config: &self.config,
            idempotent: false,
        }
    }

    pub(crate) async fn put(
        &self,
        path: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let PutOptions {
            mode,
            // not supported by GCP
            tags: _,
            attributes,
            extensions,
        } = opts;

        let builder = self
            .request(Method::PUT, path)
            .with_payload(payload)
            .with_attributes(attributes)
            .with_extensions(extensions);

        let builder = match &mode {
            PutMode::Overwrite => builder.idempotent(true),
            PutMode::Create => builder.header(&VERSION_MATCH, "0"),
            PutMode::Update(v) => {
                let etag = v.version.as_ref().ok_or(Error::MissingVersion)?;
                builder.header(&VERSION_MATCH, etag)
            }
        };

        match (mode, builder.do_put().await) {
            (PutMode::Create, Err(crate::Error::Precondition { path, source })) => {
                Err(crate::Error::AlreadyExists { path, source })
            }
            (_, r) => r,
        }
    }

    /// Perform a put part request <https://cloud.google.com/storage/docs/xml-api/put-object-multipart>
    ///
    /// Returns the new [`PartId`]
    pub(crate) async fn put_part(
        &self,
        path: &Path,
        upload_id: &MultipartId,
        part_idx: usize,
        data: PutPayload,
    ) -> Result<PartId> {
        let query = &[
            ("partNumber", &format!("{}", part_idx + 1)),
            ("uploadId", upload_id),
        ];
        let result = self
            .request(Method::PUT, path)
            .with_payload(data)
            .query(query)
            .idempotent(true)
            .do_put()
            .await?;

        Ok(PartId {
            content_id: result.e_tag.unwrap(),
        })
    }

    /// Initiate a multipart upload <https://cloud.google.com/storage/docs/xml-api/post-object-multipart>
    pub(crate) async fn multipart_initiate(
        &self,
        path: &Path,
        opts: PutMultipartOptions,
    ) -> Result<MultipartId> {
        let PutMultipartOptions {
            // not supported by GCP
            tags: _,
            attributes,
            extensions,
        } = opts;

        let response = self
            .request(Method::POST, path)
            .with_attributes(attributes)
            .with_extensions(extensions)
            .header(&CONTENT_LENGTH, "0")
            .query(&[("uploads", "")])
            .send()
            .await?;

        let data = response
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::PutResponseBody { source })?;

        let result: InitiateMultipartUploadResult =
            quick_xml::de::from_reader(data.as_ref().reader())
                .map_err(|source| Error::InvalidPutResponse { source })?;

        Ok(result.upload_id)
    }

    /// Cleanup unused parts <https://cloud.google.com/storage/docs/xml-api/delete-multipart>
    pub(crate) async fn multipart_cleanup(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        self.client
            .request(Method::DELETE, &url)
            .with_bearer_auth(credential.as_deref())
            .header(CONTENT_TYPE, "application/octet-stream")
            .header(CONTENT_LENGTH, "0")
            .query(&[("uploadId", multipart_id)])
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|source| {
                let path = path.as_ref().into();
                Error::Request { source, path }
            })?;

        Ok(())
    }

    pub(crate) async fn multipart_complete(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        completed_parts: Vec<PartId>,
    ) -> Result<PutResult> {
        if completed_parts.is_empty() {
            // GCS doesn't allow empty multipart uploads, so fallback to regular upload.
            self.multipart_cleanup(path, multipart_id).await?;
            let result = self
                .put(path, PutPayload::new(), Default::default())
                .await?;
            return Ok(result);
        }

        let upload_id = multipart_id.clone();
        let url = self.object_url(path);

        let upload_info = CompleteMultipartUpload::from(completed_parts);
        let credential = self.get_credential().await?;

        let data = quick_xml::se::to_string(&upload_info)
            .map_err(|source| Error::InvalidPutRequest { source })?
            // We cannot disable the escaping that transforms "/" to "&quote;" :(
            // https://github.com/tafia/quick-xml/issues/362
            // https://github.com/tafia/quick-xml/issues/350
            .replace("&quot;", "\"");

        let response = self
            .client
            .request(Method::POST, &url)
            .with_bearer_auth(credential.as_deref())
            .query(&[("uploadId", upload_id)])
            .body(data)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await
            .map_err(|source| Error::CompleteMultipartRequest { source })?;

        let version = get_version(response.headers(), VERSION_HEADER)
            .map_err(|source| Error::Metadata { source })?;

        let data = response
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::CompleteMultipartResponseBody { source })?;

        let response: CompleteMultipartUploadResult = quick_xml::de::from_reader(data.reader())
            .map_err(|source| Error::InvalidMultipartResponse { source })?;

        Ok(PutResult {
            e_tag: Some(response.e_tag),
            version,
        })
    }

    /// Perform a delete request <https://cloud.google.com/storage/docs/xml-api/delete-object>
    pub(crate) async fn delete_request(&self, path: &Path) -> Result<()> {
        self.request(Method::DELETE, path).send().await?;
        Ok(())
    }

    /// Perform a multi-object delete request <https://cloud.google.com/storage/docs/xml-api/post-bucket#delete-multiple>
    ///
    /// Returns one result per entry in `paths`, in the same order. Keys that don't exist are
    /// reported as deleted.
    pub(crate) async fn bulk_delete_request(&self, paths: Vec<Path>) -> Result<Vec<Result<Path>>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let credential = self.get_credential().await?;
        let url = format!("{}/{}", self.config.base_url, self.bucket_name_encoded);
        let body = delete_objects_body(&paths).map_err(|err| crate::Error::Generic {
            store: STORE,
            source: Box::new(err),
        })?;

        // GCS rejects multi-object delete requests without a Content-MD5 or CRC32 checksum.
        let mut hasher = Md5::new();
        hasher.update(&body);

        let response = self
            .client
            .request(Method::POST, &url)
            .with_bearer_auth(credential.as_deref())
            .query(&[("delete", "")])
            .header(CONTENT_TYPE, "application/xml")
            .header("Content-MD5", BASE64_STANDARD.encode(hasher.finalize()))
            .body(body)
            .retryable(&self.config.retry_config)
            .idempotent(true)
            .send()
            .await;

        let response = match response {
            // Some GCS-compatible servers, such as emulators, don't implement multi-object delete.
            Err(source)
                if matches!(
                    source.status(),
                    Some(StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED)
                ) =>
            {
                return Ok(self.delete_each(paths).await);
            }
            response => response.map_err(|source| Error::DeleteObjectsRequest {
                source,
                paths: paths.iter().map(|p| p.to_string()).collect(),
            })?,
        };

        let response = response
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::DeleteObjectsResponse { source })?;

        let response: BatchDeleteResponse =
            quick_xml::de::from_reader(response.reader()).map_err(|err| {
                Error::InvalidDeleteObjectsResponse {
                    source: Box::new(err),
                }
            })?;

        Ok(response
            .into_results(&paths)
            .map_err(|err| Error::InvalidDeleteObjectsResponse {
                source: Box::new(err),
            })?
            .into_iter()
            .map(|result| result.map_err(|error| Error::from(error).into()))
            .collect())
    }

    /// Deletes each of `paths` with its own request, up to 10 at a time.
    async fn delete_each(&self, paths: Vec<Path>) -> Vec<Result<Path>> {
        futures_util::stream::iter(paths)
            .map(|path| async move { self.delete_request(&path).await.map(|_| path) })
            .buffered(10)
            .collect()
            .await
    }

    /// Perform a copy request <https://cloud.google.com/storage/docs/xml-api/put-object-copy>
    pub(crate) async fn copy_request(&self, from: &Path, to: &Path, mode: CopyMode) -> Result<()> {
        let credential = self.get_credential().await?;
        let url = self.object_url(to);

        let from = utf8_percent_encode(from.as_ref(), NON_ALPHANUMERIC);
        let source = format!("{}/{}", self.bucket_name_encoded, from);

        let mut builder = self
            .client
            .request(Method::PUT, url)
            .header("x-goog-copy-source", source);

        let if_not_exists = match mode {
            CopyMode::Create => true,
            CopyMode::Overwrite => false,
        };

        if if_not_exists {
            builder = builder.header(&VERSION_MATCH, 0);
        }

        builder
            .with_bearer_auth(credential.as_deref())
            // Needed if reqwest is compiled with native-tls instead of rustls-tls
            // See https://github.com/apache/arrow-rs/pull/3921
            .header(CONTENT_LENGTH, 0)
            .retryable(&self.config.retry_config)
            .idempotent(!if_not_exists)
            .send()
            .await
            .map_err(|err| match err.status() {
                Some(StatusCode::PRECONDITION_FAILED) => crate::Error::AlreadyExists {
                    source: Box::new(err),
                    path: to.to_string(),
                },
                _ => err.error(STORE, from.to_string()),
            })?;

        Ok(())
    }
}

#[async_trait]
impl GetClient for GoogleCloudStorageClient {
    const STORE: &'static str = STORE;
    const HEADER_CONFIG: HeaderConfig = HeaderConfig {
        etag_required: true,
        last_modified_required: true,
        version_header: Some(VERSION_HEADER),
        user_defined_metadata_prefix: Some(USER_DEFINED_METADATA_HEADER_PREFIX),
    };

    fn retry_config(&self) -> &RetryConfig {
        &self.config.retry_config
    }

    /// Perform a get request <https://cloud.google.com/storage/docs/xml-api/get-object-download>
    async fn get_request(
        &self,
        ctx: &mut RetryContext,
        path: &Path,
        options: GetOptions,
    ) -> Result<HttpResponse> {
        let credential = self.get_credential().await?;
        let url = self.object_url(path);

        let method = match options.head {
            true => Method::HEAD,
            false => Method::GET,
        };

        let mut request = self.client.request(method, url);

        if let Some(version) = &options.version {
            request = request.query(&[("generation", version)]);
        }

        let response = request
            .with_bearer_auth(credential.as_deref())
            .with_get_options(options)
            .retryable_request()
            .send(ctx)
            .await
            .map_err(|source| {
                let path = path.as_ref().into();
                Error::GetRequest { source, path }
            })?;

        Ok(response)
    }
}

#[async_trait]
impl ListClient for Arc<GoogleCloudStorageClient> {
    /// Perform a list request <https://cloud.google.com/storage/docs/xml-api/get-bucket-list>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let credential = self.get_credential().await?;
        let url = format!("{}/{}", self.config.base_url, self.bucket_name_encoded);

        // Read before `opts.extensions` is moved into the request builder
        let invalid_key_handling = opts.invalid_keys;

        let mut query = Vec::with_capacity(5);
        query.push(("list-type", "2"));
        if let Some(delimiter) = &opts.delimiter {
            query.push(("delimiter", delimiter.as_ref()))
        }

        if let Some(prefix) = prefix {
            query.push(("prefix", prefix))
        }

        if let Some(page_token) = &opts.page_token {
            query.push(("continuation-token", page_token))
        }

        if let Some(max_results) = &self.max_list_results {
            query.push(("max-keys", max_results))
        }

        if let Some(offset) = &opts.offset {
            query.push(("start-after", offset.as_ref()))
        }

        let max_keys_str;
        if let Some(max_keys) = &opts.max_keys {
            max_keys_str = max_keys.to_string();
            query.push(("max-keys", max_keys_str.as_ref()))
        }

        let response = self
            .client
            .request(Method::GET, url)
            .extensions(opts.extensions)
            .query(&query)
            .with_bearer_auth(credential.as_deref())
            .send_retry(&self.config.retry_config)
            .await
            .map_err(|source| Error::ListRequest {
                source,
                path: prefix.unwrap_or_default().to_string(),
            })?
            .into_body()
            .bytes()
            .await
            .map_err(|source| Error::ListResponseBody { source })?;

        let mut response: ListResponse = quick_xml::de::from_reader(response.reader())
            .map_err(|source| Error::InvalidListResponse { source })?;

        let token = response.next_continuation_token.take();

        let (result, invalid_keys) = to_list_result(response, invalid_key_handling)?;

        Ok(PaginatedListResult {
            result,
            page_token: token,
            invalid_keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock_server::MockServer;
    use crate::client::retry::RetryError;
    use crate::gcp::GoogleCloudStorageBuilder;
    use crate::{ObjectStore, ObjectStoreExt};
    use http_body_util::BodyExt;
    use hyper::Response;
    use reqwest::StatusCode;

    fn mock_store(mock: &MockServer) -> crate::gcp::GoogleCloudStorage {
        let service_account = format!(
            r#"{{"gcs_base_url": "{}", "disable_oauth": true, "client_email": "", "private_key": "", "private_key_id": ""}}"#,
            mock.url()
        );
        GoogleCloudStorageBuilder::new()
            .with_bucket_name("bucket")
            .with_service_account_key(service_account)
            .build()
            .unwrap()
    }

    async fn delete_all(
        store: &crate::gcp::GoogleCloudStorage,
        paths: &[&str],
    ) -> Vec<Result<Path>> {
        let paths: Vec<_> = paths.iter().map(|p| Ok(Path::from(*p))).collect();
        store
            .delete_stream(futures_util::stream::iter(paths).boxed())
            .collect()
            .await
    }

    #[test]
    fn delete_results_follow_input_order() {
        let paths = [Path::from("a"), Path::from("b"), Path::from("c")];
        let response: BatchDeleteResponse = quick_xml::de::from_str(
            "<DeleteResult><Error><Key>c</Key><Code>AccessDenied</Code><Message>no</Message>\
             </Error><Deleted><Key>a</Key></Deleted></DeleteResult>",
        )
        .unwrap();

        let results = response.into_results(&paths).unwrap();

        assert_eq!(results[0].as_ref().unwrap(), &paths[0]);
        assert_eq!(results[1].as_ref().unwrap(), &paths[1]);
        assert_eq!(results[2].as_ref().unwrap_err().code, "AccessDenied");
    }

    #[test]
    fn quiet_delete_response_has_no_errors() {
        let paths = [Path::from("a")];
        let response: BatchDeleteResponse = quick_xml::de::from_str(
            "<DeleteResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'/>",
        )
        .unwrap();

        let results = response.into_results(&paths).unwrap();

        assert_eq!(results[0].as_ref().unwrap(), &paths[0]);
    }

    #[test]
    fn delete_response_with_unexpected_key_is_rejected() {
        let paths = [Path::from("a")];
        let response: BatchDeleteResponse = quick_xml::de::from_str(
            "<DeleteResult><Error><Key>z</Key><Code>X</Code><Message>y</Message></Error>\
             </DeleteResult>",
        )
        .unwrap();

        assert!(response.into_results(&paths).is_err());
    }

    #[tokio::test]
    async fn bulk_delete_sends_one_request_and_maps_errors() {
        let mock = MockServer::new().await;
        mock.push_async_fn(|req| async move {
            assert_eq!(req.method(), Method::POST);
            assert_eq!(req.uri().path(), "/bucket");
            assert!(req.uri().query().unwrap().starts_with("delete"));
            let content_md5 = req.headers()["Content-MD5"].to_str().unwrap().to_owned();
            let body = req.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(content_md5, BASE64_STANDARD.encode(Md5::digest(&body)));
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("<Key>a</Key>"), "{body}");
            assert!(body.contains("<Key>b &amp; c</Key>"), "{body}");
            Response::new(
                "<DeleteResult><Deleted><Key>a</Key></Deleted><Error><Key>b &amp; c</Key>\
                 <Code>ObjectUnderActiveHold</Code><Message>held</Message></Error>\
                 <Deleted><Key>missing</Key></Deleted></DeleteResult>"
                    .to_string(),
            )
        });

        let results = delete_all(&mock_store(&mock), &["a", "b & c", "missing"]).await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &Path::from("a"));
        let err = results[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("ObjectUnderActiveHold"), "{err}");
        assert_eq!(results[2].as_ref().unwrap(), &Path::from("missing"));
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn bulk_delete_falls_back_to_single_deletes_when_unsupported() {
        let mock = MockServer::new().await;
        mock.push(
            Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .body(String::new())
                .unwrap(),
        );
        for _ in 0..2 {
            mock.push_fn(|req| {
                assert_eq!(req.method(), Method::DELETE);
                Response::builder()
                    .status(StatusCode::NO_CONTENT)
                    .body(String::new())
                    .unwrap()
            });
        }

        let results: Vec<Path> = delete_all(&mock_store(&mock), &["a", "b"])
            .await
            .into_iter()
            .collect::<Result<_>>()
            .unwrap();

        assert_eq!(results, vec![Path::from("a"), Path::from("b")]);
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn single_path_delete_reports_not_found() {
        let mock = MockServer::new().await;
        mock.push_fn(|req| {
            assert_eq!(req.method(), Method::DELETE);
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(String::new())
                .unwrap()
        });

        let err = mock_store(&mock)
            .delete(&Path::from("missing"))
            .await
            .unwrap_err();

        assert!(matches!(err, crate::Error::NotFound { .. }), "{err}");
        mock.shutdown().await;
    }

    /// A failed list request must be classified by status, not flattened into `Generic`
    #[test]
    fn list_request_error_is_typed() {
        let classify = |status| {
            let source = RetryError::from_status(status);
            let path = "logs/".to_string();
            crate::Error::from(Error::ListRequest { source, path })
        };

        assert!(matches!(
            classify(StatusCode::FORBIDDEN),
            crate::Error::PermissionDenied { ref path, .. } if path == "logs/"
        ));
        assert!(matches!(
            classify(StatusCode::UNAUTHORIZED),
            crate::Error::Unauthenticated { ref path, .. } if path == "logs/"
        ));
        assert!(matches!(
            classify(StatusCode::NOT_FOUND),
            crate::Error::NotFound { ref path, .. } if path == "logs/"
        ));
        assert!(matches!(
            classify(StatusCode::INTERNAL_SERVER_ERROR),
            crate::Error::Generic { .. }
        ));
    }
}
