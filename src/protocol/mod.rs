pub mod api_version {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ApiVersion {
        pub api_key: i16,
        pub min_version: i16,
        pub max_version: i16,
    }

    impl ApiVersion {
        pub fn new(api_key: i16, min_version: i16, max_version: i16) -> Self {
            Self {
                api_key,
                min_version,
                max_version,
            }
        }

        pub fn matches(&self, api_key: i16, api_version: i16) -> bool {
            self.api_key == api_key
                && api_version >= self.min_version
                && api_version <= self.max_version
        }

        pub fn to_bytes(self) -> Vec<u8> {
            let mut buffer = Vec::with_capacity(7);
            buffer.extend_from_slice(&self.api_key.to_be_bytes());
            buffer.extend_from_slice(&self.min_version.to_be_bytes());
            buffer.extend_from_slice(&self.max_version.to_be_bytes());
            buffer.push(0);
            buffer
        }
    }
}

pub mod header {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct RequestHeader {
        pub request_api_key: i16,
        pub request_api_version: i16,
        pub correlation_id: i32,
        pub client_id: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ResponseHeader {
        pub correlation_id: i32,
    }

    impl ResponseHeader {
        pub fn to_bytes(self) -> Vec<u8> {
            let mut buffer = Vec::with_capacity(5);
            buffer.extend_from_slice(&self.correlation_id.to_be_bytes());
            buffer.push(0);
            buffer
        }
    }
}

pub mod api_versions {
    use super::api_version::ApiVersion;
    use super::header::ResponseHeader;
    use std::convert::TryFrom;

    const ERROR_NONE: i16 = 0;
    const ERROR_UNSUPPORTED_VERSION: i16 = 35;
    const DEFAULT_THROTTLE_MS: i32 = 0;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ApiVersionsRequest {
        pub api_key: i16,
        pub api_version: i16,
        pub correlation_id: i32,
        pub client_id: Option<String>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ApiVersionsResponseBody {
        error_code: i16,
        api_versions: Vec<ApiVersion>,
        throttle_time_ms: i32,
    }

    impl ApiVersionsResponseBody {
        pub fn new(error_code: i16, api_versions: Vec<ApiVersion>, throttle_time_ms: i32) -> Self {
            Self {
                error_code,
                api_versions,
                throttle_time_ms,
            }
        }

        pub fn success(api_versions: Vec<ApiVersion>) -> Self {
            Self::new(ERROR_NONE, api_versions, DEFAULT_THROTTLE_MS)
        }

        pub fn unsupported() -> Self {
            Self::new(ERROR_UNSUPPORTED_VERSION, Vec::new(), DEFAULT_THROTTLE_MS)
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut buffer = Vec::new();

            buffer.extend_from_slice(&self.error_code.to_be_bytes());

            let version_count = u16::try_from(self.api_versions.len() + 1)
                .expect("api_versions length exceeds u16::MAX");
            buffer.extend_from_slice(&version_count.to_be_bytes());

            for version in &self.api_versions {
                buffer.extend_from_slice(&version.to_bytes());
            }

            buffer.extend_from_slice(&self.throttle_time_ms.to_be_bytes());
            buffer.push(0);

            buffer
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ApiVersionsResponse {
        header: ResponseHeader,
        body: ApiVersionsResponseBody,
    }

    impl ApiVersionsResponse {
        pub fn new(correlation_id: i32, body: ApiVersionsResponseBody) -> Self {
            Self {
                header: ResponseHeader { correlation_id },
                body,
            }
        }

        pub fn success(correlation_id: i32, api_versions: &[ApiVersion]) -> Self {
            Self::new(
                correlation_id,
                ApiVersionsResponseBody::success(api_versions.to_vec()),
            )
        }

        pub fn unsupported(correlation_id: i32) -> Self {
            Self::new(correlation_id, ApiVersionsResponseBody::unsupported())
        }

        pub fn to_bytes(&self) -> Vec<u8> {
            let mut payload = Vec::new();
            payload.extend_from_slice(&self.header.to_bytes());
            payload.extend_from_slice(&self.body.to_bytes());

            let mut buffer = Vec::with_capacity(4 + payload.len());
            buffer.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            buffer.extend_from_slice(&payload);
            buffer
        }
    }
}

pub use api_version::ApiVersion;
pub use api_versions::{ApiVersionsRequest, ApiVersionsResponse};
pub use header::RequestHeader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    ApiVersions(ApiVersionsRequest),
}
