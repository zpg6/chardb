//! `CharDB` protocol v3 wire types.
//!
//! The tag set and fields are closed at version 3. Unknown error codes and
//! refetch reasons are the two additive exceptions described by the server
//! contract.

use std::fmt;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::{operation::is_valid_reference, Error, ErrorKind, Result};

pub const PROTOCOL_VERSION: u8 = 3;
pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub const MAX_JSON_DEPTH: usize = 100;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SafeId(u64);

impl SafeId {
    /// Build an ID that can cross the TypeScript protocol without precision loss.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` exceeds JavaScript's safe integer range.
    pub fn new(value: u64) -> Result<Self> {
        if value <= MAX_SAFE_INTEGER {
            Ok(Self(value))
        } else {
            Err(Error::local(
                ErrorKind::Configuration,
                format!("wire id {value} exceeds JavaScript's safe integer range"),
            ))
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl Serialize for SafeId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for SafeId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        if value > MAX_SAFE_INTEGER {
            return Err(de::Error::custom(
                "wire id exceeds JavaScript's safe integer range",
            ));
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum CdbErrorCode {
    StaleEpoch,
    CrossPartition,
    CrossPartitionBatch,
    InteractiveTxnUnsupported,
    NonlocalUnique,
    NonlocalFk,
    AmbiguousColocation,
    PolicyUnknownRoot,
    PartitionContractChanged,
    ScatterNotIndex,
    MutIdCollision,
    UniqueViolation,
    MutationOutcomeUnknown,
    OplogPressure,
    AuthProfileIncompatible,
    ReservedTableName,
    NoIntentForRawSql,
    TxnAbortedEviction,
    ShardsChanged,
    RateLimited,
    CallerDenied,
    UnsupportedFeature,
    ShardUnavailable,
    CatalogUnavailable,
    Forbidden,
    GsiStrictRequires2pc,
    VectorizeIndexMissing,
    VectorizeDimMismatch,
    StreamAborted,
    DistinctCapExceeded,
    Invariant,
    ReshardPhaseMismatch,
    RefNotFound,
    DtNotImplemented,
    DtAborted,
    AuthNotBound,
    AuthCrossPartitionTx,
    AuthGsiMiss,
    NotCdbTable,
    InvalidArgs,
    InvalidTenant,
    InvalidSelf,
    InvalidColumn,
    InvalidPartition,
    AmbiguousTenant,
    MissingTenantFk,
    PolicyConflict,
    ForbiddenColumn,
    TenantOverride,
    SelfOverride,
}

impl CdbErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StaleEpoch => "CDB_STALE_EPOCH",
            Self::CrossPartition => "CDB_CROSS_PARTITION",
            Self::CrossPartitionBatch => "CDB_CROSS_PARTITION_BATCH",
            Self::InteractiveTxnUnsupported => "CDB_INTERACTIVE_TXN_UNSUPPORTED",
            Self::NonlocalUnique => "CDB_NONLOCAL_UNIQUE",
            Self::NonlocalFk => "CDB_NONLOCAL_FK",
            Self::AmbiguousColocation => "CDB_AMBIGUOUS_COLOCATION",
            Self::PolicyUnknownRoot => "CDB_POLICY_UNKNOWN_ROOT",
            Self::PartitionContractChanged => "CDB_PARTITION_CONTRACT_CHANGED",
            Self::ScatterNotIndex => "CDB_SCATTER_NOT_INDEX",
            Self::MutIdCollision => "CDB_MUT_ID_COLLISION",
            Self::UniqueViolation => "CDB_UNIQUE_VIOLATION",
            Self::MutationOutcomeUnknown => "CDB_MUTATION_OUTCOME_UNKNOWN",
            Self::OplogPressure => "CDB_OPLOG_PRESSURE",
            Self::AuthProfileIncompatible => "CDB_AUTH_PROFILE_INCOMPATIBLE",
            Self::ReservedTableName => "CDB_RESERVED_TABLE_NAME",
            Self::NoIntentForRawSql => "CDB_NO_INTENT_FOR_RAW_SQL",
            Self::TxnAbortedEviction => "CDB_TXN_ABORTED_EVICTION",
            Self::ShardsChanged => "CDB_SHARDS_CHANGED",
            Self::RateLimited => "CDB_RATE_LIMITED",
            Self::CallerDenied => "CDB_CALLER_DENIED",
            Self::UnsupportedFeature => "CDB_UNSUPPORTED_FEATURE",
            Self::ShardUnavailable => "CDB_SHARD_UNAVAILABLE",
            Self::CatalogUnavailable => "CDB_CATALOG_UNAVAILABLE",
            Self::Forbidden => "CDB_FORBIDDEN",
            Self::GsiStrictRequires2pc => "CDB_GSI_STRICT_REQUIRES_2PC",
            Self::VectorizeIndexMissing => "CDB_VECTORIZE_INDEX_MISSING",
            Self::VectorizeDimMismatch => "CDB_VECTORIZE_DIM_MISMATCH",
            Self::StreamAborted => "CDB_STREAM_ABORTED",
            Self::DistinctCapExceeded => "CDB_DISTINCT_CAP_EXCEEDED",
            Self::Invariant => "CDB_INVARIANT",
            Self::ReshardPhaseMismatch => "CDB_RESHARD_PHASE_MISMATCH",
            Self::RefNotFound => "CDB_REF_NOT_FOUND",
            Self::DtNotImplemented => "CDB_DT_NOT_IMPLEMENTED",
            Self::DtAborted => "CDB_DT_ABORTED",
            Self::AuthNotBound => "CDB_AUTH_NOT_BOUND",
            Self::AuthCrossPartitionTx => "CDB_AUTH_CROSS_PARTITION_TX",
            Self::AuthGsiMiss => "CDB_AUTH_GSI_MISS",
            Self::NotCdbTable => "CDB_NOT_CDB_TABLE",
            Self::InvalidArgs => "CDB_INVALID_ARGS",
            Self::InvalidTenant => "CDB_INVALID_TENANT",
            Self::InvalidSelf => "CDB_INVALID_SELF",
            Self::InvalidColumn => "CDB_INVALID_COLUMN",
            Self::InvalidPartition => "CDB_INVALID_PARTITION",
            Self::AmbiguousTenant => "CDB_AMBIGUOUS_TENANT",
            Self::MissingTenantFk => "CDB_MISSING_TENANT_FK",
            Self::PolicyConflict => "CDB_POLICY_CONFLICT",
            Self::ForbiddenColumn => "CDB_FORBIDDEN_COLUMN",
            Self::TenantOverride => "CDB_TENANT_OVERRIDE",
            Self::SelfOverride => "CDB_SELF_OVERRIDE",
        }
    }

    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::StaleEpoch
                | Self::TxnAbortedEviction
                | Self::RateLimited
                | Self::ShardUnavailable
                | Self::CatalogUnavailable
                | Self::StreamAborted
        )
    }

    #[must_use]
    pub fn docs_url(self) -> String {
        format!(
            "https://chardb.dev/errors/{}",
            self.as_str().to_ascii_lowercase()
        )
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "CDB_STALE_EPOCH" => Self::StaleEpoch,
            "CDB_CROSS_PARTITION" => Self::CrossPartition,
            "CDB_CROSS_PARTITION_BATCH" => Self::CrossPartitionBatch,
            "CDB_INTERACTIVE_TXN_UNSUPPORTED" => Self::InteractiveTxnUnsupported,
            "CDB_NONLOCAL_UNIQUE" => Self::NonlocalUnique,
            "CDB_NONLOCAL_FK" => Self::NonlocalFk,
            "CDB_AMBIGUOUS_COLOCATION" => Self::AmbiguousColocation,
            "CDB_POLICY_UNKNOWN_ROOT" => Self::PolicyUnknownRoot,
            "CDB_PARTITION_CONTRACT_CHANGED" => Self::PartitionContractChanged,
            "CDB_SCATTER_NOT_INDEX" => Self::ScatterNotIndex,
            "CDB_MUT_ID_COLLISION" => Self::MutIdCollision,
            "CDB_UNIQUE_VIOLATION" => Self::UniqueViolation,
            "CDB_MUTATION_OUTCOME_UNKNOWN" => Self::MutationOutcomeUnknown,
            "CDB_OPLOG_PRESSURE" => Self::OplogPressure,
            "CDB_AUTH_PROFILE_INCOMPATIBLE" => Self::AuthProfileIncompatible,
            "CDB_RESERVED_TABLE_NAME" => Self::ReservedTableName,
            "CDB_NO_INTENT_FOR_RAW_SQL" => Self::NoIntentForRawSql,
            "CDB_TXN_ABORTED_EVICTION" => Self::TxnAbortedEviction,
            "CDB_SHARDS_CHANGED" => Self::ShardsChanged,
            "CDB_RATE_LIMITED" => Self::RateLimited,
            "CDB_CALLER_DENIED" => Self::CallerDenied,
            "CDB_UNSUPPORTED_FEATURE" => Self::UnsupportedFeature,
            "CDB_SHARD_UNAVAILABLE" => Self::ShardUnavailable,
            "CDB_CATALOG_UNAVAILABLE" => Self::CatalogUnavailable,
            "CDB_FORBIDDEN" => Self::Forbidden,
            "CDB_GSI_STRICT_REQUIRES_2PC" => Self::GsiStrictRequires2pc,
            "CDB_VECTORIZE_INDEX_MISSING" => Self::VectorizeIndexMissing,
            "CDB_VECTORIZE_DIM_MISMATCH" => Self::VectorizeDimMismatch,
            "CDB_STREAM_ABORTED" => Self::StreamAborted,
            "CDB_DISTINCT_CAP_EXCEEDED" => Self::DistinctCapExceeded,
            "CDB_INVARIANT" => Self::Invariant,
            "CDB_RESHARD_PHASE_MISMATCH" => Self::ReshardPhaseMismatch,
            "CDB_REF_NOT_FOUND" => Self::RefNotFound,
            "CDB_DT_NOT_IMPLEMENTED" => Self::DtNotImplemented,
            "CDB_DT_ABORTED" => Self::DtAborted,
            "CDB_AUTH_NOT_BOUND" => Self::AuthNotBound,
            "CDB_AUTH_CROSS_PARTITION_TX" => Self::AuthCrossPartitionTx,
            "CDB_AUTH_GSI_MISS" => Self::AuthGsiMiss,
            "CDB_NOT_CDB_TABLE" => Self::NotCdbTable,
            "CDB_INVALID_ARGS" => Self::InvalidArgs,
            "CDB_INVALID_TENANT" => Self::InvalidTenant,
            "CDB_INVALID_SELF" => Self::InvalidSelf,
            "CDB_INVALID_COLUMN" => Self::InvalidColumn,
            "CDB_INVALID_PARTITION" => Self::InvalidPartition,
            "CDB_AMBIGUOUS_TENANT" => Self::AmbiguousTenant,
            "CDB_MISSING_TENANT_FK" => Self::MissingTenantFk,
            "CDB_POLICY_CONFLICT" => Self::PolicyConflict,
            "CDB_FORBIDDEN_COLUMN" => Self::ForbiddenColumn,
            "CDB_TENANT_OVERRIDE" => Self::TenantOverride,
            "CDB_SELF_OVERRIDE" => Self::SelfOverride,
            _ => return None,
        })
    }
}

impl fmt::Display for CdbErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CdbErrorCode {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CdbErrorCode {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(Self::parse(&value).unwrap_or(Self::Invariant))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RefetchReason {
    Lagged,
    AuthChanged,
    SchemaChanged,
    ProtocolMismatch,
    ShardsChanged,
    PitrIdempotencyReset,
    GsiLag,
}

impl RefetchReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lagged => "lagged",
            Self::AuthChanged => "authChanged",
            Self::SchemaChanged => "schemaChanged",
            Self::ProtocolMismatch => "protocolMismatch",
            Self::ShardsChanged => "shardsChanged",
            Self::PitrIdempotencyReset => "pitrIdempotencyReset",
            Self::GsiLag => "gsiLag",
        }
    }
}

impl Serialize for RefetchReason {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for RefetchReason {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(match value.as_str() {
            "authChanged" => Self::AuthChanged,
            "schemaChanged" => Self::SchemaChanged,
            "protocolMismatch" => Self::ProtocolMismatch,
            "shardsChanged" => Self::ShardsChanged,
            "pitrIdempotencyReset" => Self::PitrIdempotencyReset,
            "gsiLag" => Self::GsiLag,
            _ => Self::Lagged,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", deny_unknown_fields)]
pub enum Up {
    #[serde(rename = "hello")]
    Hello {
        #[serde(rename = "protocolV")]
        protocol_v: u8,
        #[serde(rename = "clientId")]
        client_id: String,
        #[serde(rename = "resume", skip_serializing_if = "Option::is_none")]
        resume: Option<String>,
        #[serde(rename = "resumeFromCookie", skip_serializing_if = "Option::is_none")]
        resume_from_cookie: Option<String>,
        jwt: String,
    },
    #[serde(rename = "sub")]
    Subscribe {
        #[serde(rename = "subId")]
        sub_id: SafeId,
        r#ref: String,
        args: Value,
        #[serde(rename = "ttlMs", skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<Value>,
    },
    #[serde(rename = "unsub")]
    Unsubscribe {
        #[serde(rename = "subId")]
        sub_id: SafeId,
    },
    #[serde(rename = "mut")]
    Mutate {
        #[serde(rename = "mutId")]
        mutation_id: String,
        r#ref: String,
        args: Value,
    },
    #[serde(rename = "updateAuth")]
    UpdateAuth { jwt: String },
    #[serde(rename = "ack")]
    Ack { cookie: String },
    #[serde(rename = "ping")]
    Ping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RowPatchOp {
    Put,
    Del,
    Edit,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowPatch {
    pub op: RowPatchOp,
    #[serde(rename = "subId")]
    pub sub_id: SafeId,
    #[serde(rename = "rowKey")]
    pub row_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteError {
    pub code: CdbErrorCode,
    pub retryable: bool,
    pub docs: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MutationResult {
    Success {
        mutation_id: String,
        result: Value,
        cookie: String,
    },
    Failure {
        mutation_id: String,
        error: RemoteError,
    },
}

impl<'de> Deserialize<'de> for MutationResult {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(rename = "mutId")]
            mutation_id: String,
            ok: bool,
            result: Option<Value>,
            cookie: Option<String>,
            error: Option<RemoteError>,
        }

        let raw = Raw::deserialize(deserializer)?;
        if raw.ok {
            match (raw.result, raw.cookie, raw.error) {
                (Some(result), Some(cookie), None) => Ok(Self::Success {
                    mutation_id: raw.mutation_id,
                    result,
                    cookie,
                }),
                _ => Err(de::Error::custom(
                    "successful mutation result has invalid fields",
                )),
            }
        } else {
            match (raw.result, raw.cookie, raw.error) {
                (None, None, Some(error)) => Ok(Self::Failure {
                    mutation_id: raw.mutation_id,
                    error,
                }),
                _ => Err(de::Error::custom(
                    "failed mutation result has invalid fields",
                )),
            }
        }
    }
}

impl Serialize for MutationResult {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        match self {
            Self::Success {
                mutation_id,
                result,
                cookie,
            } => {
                let mut state = serializer.serialize_struct("MutationResult", 4)?;
                state.serialize_field("mutId", mutation_id)?;
                state.serialize_field("ok", &true)?;
                state.serialize_field("result", result)?;
                state.serialize_field("cookie", cookie)?;
                state.end()
            }
            Self::Failure { mutation_id, error } => {
                let mut state = serializer.serialize_struct("MutationResult", 3)?;
                state.serialize_field("mutId", mutation_id)?;
                state.serialize_field("ok", &false)?;
                state.serialize_field("error", error)?;
                state.end()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", deny_unknown_fields)]
pub enum Down {
    #[serde(rename = "welcome")]
    Welcome {
        #[serde(rename = "protocolV")]
        protocol_v: u8,
        #[serde(rename = "baseCookie")]
        base_cookie: String,
        region: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        colo: Option<String>,
        #[serde(rename = "resumedFromCookie", skip_serializing_if = "Option::is_none")]
        resumed_from_cookie: Option<String>,
    },
    #[serde(rename = "poke")]
    Poke {
        cookie: String,
        patches: Vec<RowPatch>,
        #[serde(rename = "mutResults", skip_serializing_if = "Option::is_none")]
        mutation_results: Option<Vec<MutationResult>>,
    },
    #[serde(rename = "snapshot")]
    Snapshot {
        #[serde(rename = "subId")]
        sub_id: SafeId,
        cookie: String,
        rows: Vec<Value>,
    },
    #[serde(rename = "mustRefetch")]
    MustRefetch {
        #[serde(rename = "subIds")]
        sub_ids: Vec<SafeId>,
        reason: RefetchReason,
    },
    #[serde(rename = "error")]
    Error {
        code: CdbErrorCode,
        #[serde(rename = "subId", skip_serializing_if = "Option::is_none")]
        sub_id: Option<SafeId>,
        retryable: bool,
        #[serde(rename = "correlationId")]
        correlation_id: String,
        docs: String,
    },
}

/// Encode and validate one client-to-server message.
///
/// # Errors
///
/// Returns an error for an invalid message or failed JSON serialization.
pub fn encode_up(message: &Up) -> Result<String> {
    validate_up(message)?;
    serde_json::to_string(message).map_err(|error| {
        Error::local(
            ErrorKind::Protocol,
            format!("failed to encode wire message: {error}"),
        )
    })
}

/// Decode and validate one client-to-server message.
///
/// # Errors
///
/// Returns an error for malformed JSON or a protocol-v3 contract violation.
pub fn decode_up(raw: &str) -> Result<Up> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        Error::local(ErrorKind::Protocol, format!("invalid wire JSON: {error}"))
    })?;
    validate_json(&value, MAX_JSON_DEPTH)?;
    let message: Up = serde_json::from_value(value).map_err(|error| {
        Error::local(
            ErrorKind::Protocol,
            format!("malformed client message: {error}"),
        )
    })?;
    validate_up(&message)?;
    Ok(message)
}

/// Decode and validate one server-to-client message.
///
/// # Errors
///
/// Returns an error for malformed JSON or a protocol-v3 contract violation.
pub fn decode_down(raw: &str) -> Result<Down> {
    let mut value: Value = serde_json::from_str(raw).map_err(|error| {
        Error::local(ErrorKind::Protocol, format!("invalid wire JSON: {error}"))
    })?;
    validate_json(&value, MAX_JSON_DEPTH)?;
    normalize_additive_error_codes(&mut value);
    let message: Down = serde_json::from_value(value).map_err(|error| {
        Error::local(
            ErrorKind::Protocol,
            format!("malformed server message: {error}"),
        )
    })?;
    validate_down(&message)?;
    Ok(message)
}

fn normalize_additive_error_codes(value: &mut Value) {
    fn normalize(object: &mut serde_json::Map<String, Value>) {
        let Some(Value::String(code)) = object.get("code") else {
            return;
        };
        if CdbErrorCode::parse(code).is_some() {
            return;
        }
        object.insert("code".to_owned(), Value::String("CDB_INVARIANT".to_owned()));
        object.insert("retryable".to_owned(), Value::Bool(false));
        object.insert(
            "docs".to_owned(),
            Value::String(CdbErrorCode::Invariant.docs_url()),
        );
    }

    let Some(root) = value.as_object_mut() else {
        return;
    };
    normalize(root);
    if let Some(Value::Array(results)) = root.get_mut("mutResults") {
        for result in results {
            if let Some(error) = result.get_mut("error").and_then(Value::as_object_mut) {
                normalize(error);
            }
        }
    }
    if let Some(error) = root
        .get_mut("finalMutResult")
        .and_then(|result| result.get_mut("error"))
        .and_then(Value::as_object_mut)
    {
        normalize(error);
    }
}

fn validate_up(message: &Up) -> Result<()> {
    match message {
        Up::Hello {
            protocol_v,
            client_id,
            jwt,
            ..
        } => {
            if *protocol_v != PROTOCOL_VERSION {
                return Err(Error::local(
                    ErrorKind::Protocol,
                    "hello protocolV must equal 3",
                ));
            }
            if client_id.is_empty() || jwt.is_empty() {
                return Err(Error::local(
                    ErrorKind::Configuration,
                    "clientId and jwt must not be empty",
                ));
            }
        }
        Up::Subscribe {
            r#ref,
            args,
            ttl_ms,
            ..
        } => {
            validate_reference(r#ref)?;
            validate_json(args, MAX_JSON_DEPTH)?;
            validate_nonnegative_number(ttl_ms.as_ref(), "ttlMs")?;
        }
        Up::Mutate { r#ref, args, .. } => {
            validate_reference(r#ref)?;
            validate_json(args, MAX_JSON_DEPTH)?;
        }
        Up::Unsubscribe { .. } | Up::UpdateAuth { .. } | Up::Ack { .. } | Up::Ping => {}
    }
    Ok(())
}

fn validate_down(message: &Down) -> Result<()> {
    match message {
        Down::Welcome { protocol_v, .. } if *protocol_v != PROTOCOL_VERSION => Err(Error::local(
            ErrorKind::Protocol,
            format!("server selected unsupported protocol version {protocol_v}"),
        )),
        Down::Poke {
            mutation_results: Some(results),
            ..
        } => {
            for result in results {
                if let MutationResult::Failure { error, .. } = result {
                    validate_remote_error(error)?;
                }
            }
            Ok(())
        }
        Down::Error {
            code, retryable, ..
        } if *retryable != code.is_retryable() => Err(Error::local(
            ErrorKind::Protocol,
            format!("retryable polarity does not match {}", code.as_str()),
        )),
        _ => Ok(()),
    }
}

fn validate_remote_error(error: &RemoteError) -> Result<()> {
    if error.retryable != error.code.is_retryable() {
        return Err(Error::local(
            ErrorKind::Protocol,
            format!("retryable polarity does not match {}", error.code.as_str()),
        ));
    }
    Ok(())
}

pub(crate) fn validate_reference(reference: &str) -> Result<()> {
    if !is_valid_reference(reference) {
        return Err(Error::local(
            ErrorKind::Configuration,
            "CharDB reference must be nonempty and contain '#'",
        ));
    }
    Ok(())
}

fn validate_nonnegative_number(value: Option<&Value>, name: &str) -> Result<()> {
    if value.is_some_and(|value| {
        value
            .as_f64()
            .is_none_or(|number| !number.is_finite() || number < 0.0)
    }) {
        return Err(Error::local(
            ErrorKind::Configuration,
            format!("{name} must be finite and nonnegative"),
        ));
    }
    Ok(())
}

pub(crate) fn validate_json(value: &Value, max_depth: usize) -> Result<()> {
    fn visit(value: &Value, depth: usize, max_depth: usize) -> Result<()> {
        if depth > max_depth {
            return Err(Error::local(
                ErrorKind::Protocol,
                format!("JSON nesting exceeds {max_depth} levels"),
            ));
        }
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1, max_depth)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1, max_depth)?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
        Ok(())
    }
    visit(value, 0, max_depth)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_additive_values_normalize() {
        let error: Down = decode_down(
            r#"{"t":"error","code":"CDB_FUTURE","retryable":false,"correlationId":"c","docs":"future"}"#,
        )
        .unwrap();
        assert!(matches!(
            error,
            Down::Error {
                code: CdbErrorCode::Invariant,
                ..
            }
        ));

        let refetch = decode_down(r#"{"t":"mustRefetch","subIds":[],"reason":"future"}"#).unwrap();
        assert!(matches!(
            refetch,
            Down::MustRefetch {
                reason: RefetchReason::Lagged,
                ..
            }
        ));
    }

    #[test]
    fn rejects_extra_fields_and_wrong_polarity() {
        assert!(decode_down(
            r#"{"t":"welcome","protocolV":3,"baseCookie":"c","region":"x","extra":true}"#
        )
        .is_err());
        assert!(decode_down(
            r#"{"t":"error","code":"CDB_RATE_LIMITED","retryable":false,"correlationId":"c","docs":"d"}"#
        )
        .is_err());
    }
}
