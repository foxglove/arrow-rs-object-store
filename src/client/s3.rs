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

//! The list, multipart and multi-object delete API used by both GCS and S3

use crate::client::list::parse_key;
use crate::list::{InvalidKey, InvalidKeyHandling};
use crate::multipart::PartId;
use crate::path::Path;
use crate::{ListResult, ObjectMeta, Result};
use chrono::{DateTime, Utc};
use quick_xml::events::{BytesEnd, BytesStart, BytesText, Event};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ListResponse {
    #[serde(default)]
    pub contents: Vec<ListContents>,
    #[serde(default)]
    pub common_prefixes: Vec<ListPrefix>,
    #[serde(default)]
    pub next_continuation_token: Option<String>,
}

/// Converts a list response into a [`ListResult`], along with the entries omitted
/// from it under [`InvalidKeyHandling::Skip`]
pub(crate) fn to_list_result(
    value: ListResponse,
    handling: InvalidKeyHandling,
) -> Result<(ListResult, Vec<InvalidKey>)> {
    let mut invalid_keys = Vec::new();

    let common_prefixes = value
        .common_prefixes
        .into_iter()
        .filter_map(|x| parse_key(x.prefix, handling, &mut invalid_keys).transpose())
        .collect::<Result<_>>()?;

    let objects = value
        .contents
        .into_iter()
        .filter_map(|x| to_object_meta(x, handling, &mut invalid_keys).transpose())
        .collect::<Result<_>>()?;

    Ok((
        ListResult {
            common_prefixes,
            objects,
        },
        invalid_keys,
    ))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ListPrefix {
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ListContents {
    pub key: String,
    pub size: u64,
    pub last_modified: DateTime<Utc>,
    #[serde(rename = "ETag")]
    pub e_tag: Option<String>,
}

/// Returns `Ok(None)` if the key was skipped, see [`parse_key`]
fn to_object_meta(
    value: ListContents,
    handling: InvalidKeyHandling,
    invalid_keys: &mut Vec<InvalidKey>,
) -> Result<Option<ObjectMeta>> {
    let Some(location) = parse_key(value.key, handling, invalid_keys)? else {
        return Ok(None);
    };

    Ok(Some(ObjectMeta {
        location,
        last_modified: value.last_modified,
        size: value.size,
        e_tag: value.e_tag,
        version: None,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct InitiateMultipartUploadResult {
    pub upload_id: String,
}

#[cfg(feature = "aws")]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CopyPartResult {
    #[serde(rename = "ETag")]
    pub e_tag: String,
    #[serde(default, rename = "ChecksumSHA256")]
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CompleteMultipartUpload {
    pub part: Vec<MultipartPart>,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct PartMetadata {
    pub e_tag: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

impl From<Vec<PartId>> for CompleteMultipartUpload {
    fn from(value: Vec<PartId>) -> Self {
        let part = value
            .into_iter()
            .enumerate()
            .map(|(part_idx, part)| {
                let md = match quick_xml::de::from_str::<PartMetadata>(&part.content_id) {
                    Ok(md) => md,
                    // fallback to old way
                    Err(_) => PartMetadata {
                        e_tag: part.content_id.clone(),
                        checksum_sha256: None,
                    },
                };
                MultipartPart {
                    e_tag: md.e_tag,
                    part_number: part_idx + 1,
                    checksum_sha256: md.checksum_sha256,
                }
            })
            .collect();
        Self { part }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MultipartPart {
    #[serde(rename = "ETag")]
    pub e_tag: String,
    #[serde(rename = "PartNumber")]
    pub part_number: usize,
    #[serde(rename = "ChecksumSHA256")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CompleteMultipartUploadResult {
    #[serde(rename = "ETag")]
    pub e_tag: String,
}

/// Builds the XML body of a multi-object delete request for `paths`.
pub(crate) fn delete_objects_body(paths: &[Path]) -> std::io::Result<Vec<u8>> {
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
pub(crate) struct BatchDeleteResponse {
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
pub(crate) struct DeleteError {
    pub key: String,
    pub code: String,
    pub message: String,
}

/// Error returned when a multi-object delete response names a key that wasn't requested.
#[derive(Debug, thiserror::Error)]
#[error("DeleteObjects response contains unexpected key {key}")]
pub(crate) struct UnexpectedDeleteKey {
    key: String,
}

impl BatchDeleteResponse {
    /// Returns one result per entry in `paths`, in the same order.
    ///
    /// Keys absent from the response are treated as deleted, which covers responses to
    /// requests with `Quiet` set.
    pub(crate) fn into_results(
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

#[cfg(test)]
mod tests {
    use super::*;

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

    /// A page mixing representable keys with ones `Path` cannot represent: an empty
    /// segment, a relative segment, and an ASCII control character. `CommonPrefixes`
    /// holds one of each kind too.
    const LIST_RESPONSE: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<ListBucketResult>
    <Name>bucket</Name>
    <Prefix>logs/</Prefix>
    <KeyCount>4</KeyCount>
    <MaxKeys>1000</MaxKeys>
    <Delimiter>/</Delimiter>
    <IsTruncated>true</IsTruncated>
    <NextContinuationToken>token-abc</NextContinuationToken>
    <Contents>
        <Key>logs/a.mcap</Key>
        <LastModified>2024-01-01T00:00:00.000Z</LastModified>
        <ETag>\"etag-a\"</ETag>
        <Size>100</Size>
    </Contents>
    <Contents>
        <Key>logs//b.mcap</Key>
        <LastModified>2024-01-02T00:00:00.000Z</LastModified>
        <ETag>\"etag-b\"</ETag>
        <Size>200</Size>
    </Contents>
    <Contents>
        <Key>logs/../c.mcap</Key>
        <LastModified>2024-01-03T00:00:00.000Z</LastModified>
        <ETag>\"etag-c\"</ETag>
        <Size>300</Size>
    </Contents>
    <Contents>
        <Key>logs/d\u{7}.mcap</Key>
        <LastModified>2024-01-04T00:00:00.000Z</LastModified>
        <ETag>\"etag-d\"</ETag>
        <Size>400</Size>
    </Contents>
    <CommonPrefixes>
        <Prefix>logs/good/</Prefix>
    </CommonPrefixes>
    <CommonPrefixes>
        <Prefix>bad//prefix/</Prefix>
    </CommonPrefixes>
</ListBucketResult>";

    fn parse() -> ListResponse {
        quick_xml::de::from_str(LIST_RESPONSE).unwrap()
    }

    #[test]
    fn list_result_error_on_invalid_key() {
        let err = to_list_result(parse(), InvalidKeyHandling::Error).unwrap_err();
        assert!(
            matches!(err, crate::Error::InvalidPath { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn list_result_skip_invalid_key() {
        let response = parse();
        let token = response.next_continuation_token.clone();

        let (result, invalid_keys) = to_list_result(response, InvalidKeyHandling::Skip).unwrap();

        // The continuation token survives a page that skipped entries
        assert_eq!(token.as_deref(), Some("token-abc"));

        let objects: Vec<_> = result.objects.iter().map(|x| x.location.as_ref()).collect();
        assert_eq!(objects, vec!["logs/a.mcap"]);
        assert_eq!(result.objects[0].size, 100);
        assert_eq!(result.objects[0].e_tag.as_deref(), Some("\"etag-a\""));

        let prefixes: Vec<_> = result.common_prefixes.iter().map(|x| x.as_ref()).collect();
        assert_eq!(prefixes, vec!["logs/good"]);

        // Raw keys are reported verbatim, prefixes ahead of objects
        let keys: Vec<_> = invalid_keys.iter().map(|x| x.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "bad//prefix/",
                "logs//b.mcap",
                "logs/../c.mcap",
                "logs/d\u{7}.mcap",
            ]
        );
    }
}
