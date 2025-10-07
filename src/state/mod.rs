use crate::protocol::{ApiVersion, ApiVersionsRequest, ApiVersionsResponse};

const API_VERSIONS_KEY: i16 = 18;
const SUPPORTED_MIN_VERSION: i16 = 0;
const SUPPORTED_MAX_VERSION: i16 = 4;

pub struct ApiRegistry {
    supported: Vec<ApiVersion>,
}

impl ApiRegistry {
    #[allow(dead_code)]
    pub fn new(supported: Vec<ApiVersion>) -> Self {
        Self { supported }
    }

    #[allow(dead_code)]
    pub fn supported_versions(&self) -> &[ApiVersion] {
        &self.supported
    }

    pub fn handle_versions(&self, request: ApiVersionsRequest) -> ApiVersionsResponse {
        if self.supports(request.api_key, request.api_version) {
            ApiVersionsResponse::success(request.correlation_id, &self.supported)
        } else {
            ApiVersionsResponse::unsupported(request.correlation_id)
        }
    }

    fn supports(&self, api_key: i16, api_version: i16) -> bool {
        self.supported
            .iter()
            .any(|entry| entry.matches(api_key, api_version))
    }
}

impl Default for ApiRegistry {
    fn default() -> Self {
        Self {
            supported: vec![
                ApiVersion::new(17, 0, 4),
                ApiVersion::new(18, 0, 4),
                ApiVersion::new(19, 0, 4),
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn handle_versions_returns_success_for_supported_api() {
        let registry = ApiRegistry::default();
        let request = build_request(18, 4, 42, Some("client"));
        let response = registry.handle_versions(request);

        let bytes = response.to_bytes();
        let declared_length =
            u32::from_be_bytes(bytes[0..4].try_into().expect("length prefix slice"));
        assert_eq!(declared_length as usize, bytes.len() - 4);

        let correlation_id =
            i32::from_be_bytes(bytes[4..8].try_into().expect("correlation id slice"));
        assert_eq!(correlation_id, 42);

        let error_code = i16::from_be_bytes(bytes[9..11].try_into().expect("error code slice"));
        assert_eq!(error_code, 0);

        let version_count =
            u16::from_be_bytes(bytes[11..13].try_into().expect("version count slice"));
        assert_eq!(
            usize::from(version_count),
            registry.supported_versions().len()
        );
    }

    #[test]
    fn handle_versions_returns_error_for_unsupported_api() {
        let registry = ApiRegistry::default();
        let request = build_request(99, 0, 7, None);
        let response = registry.handle_versions(request);

        let bytes = response.to_bytes();
        let error_code = i16::from_be_bytes(bytes[9..11].try_into().expect("error code slice"));
        assert_eq!(error_code, 35);

        let version_count =
            u16::from_be_bytes(bytes[11..13].try_into().expect("version count slice"));
        assert_eq!(version_count, 0);
    }

    fn build_request(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> ApiVersionsRequest {
        ApiVersionsRequest {
            api_key,
            api_version,
            correlation_id,
            client_id: client_id.map(|value| value.to_string()),
        }
    }
}
