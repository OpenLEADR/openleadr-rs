use serde::{Deserialize, Serialize};
use url::Url;
use validator::{Validate, ValidateLength, ValidationErrors};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthErrorType {
    OAuthNotEnabled,
    InvalidRequest,
    InvalidClient,
    InvalidSubject,
    InvalidGrant,
    // UnauthorizedClient,
    UnsupportedGrantType,
    // InvalidScope,
    ServerError,
    NoAvailableKeys,
    /// nbf claim set to a later time than 'now'
    NotYetValid,
    /// exp claim > now
    Expired,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct OAuthError {
    pub error: OAuthErrorType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_uri: Option<String>,
}

impl OAuthError {
    pub fn new(error: OAuthErrorType) -> Self {
        Self {
            error,
            error_description: None,
            error_uri: None,
        }
    }

    pub fn with_description(mut self, description: String) -> Self {
        self.error_description = Some(description);
        self
    }

    pub fn with_uri(mut self, uri: String) -> Self {
        self.error_uri = Some(uri);
        self
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthServerInfo {
    #[serde(rename = "tokenURL")]
    pub token_url: Url,
}

impl Validate for AuthServerInfo {
    fn validate(&self) -> Result<(), ValidationErrors> {
        if self
            .token_url
            .as_str()
            .validate_length(Some(2), Some(8000), None)
        {
            return Ok(());
        }

        let mut err = ::validator::ValidationError::new("length");
        err.add_param(::std::borrow::Cow::from("min"), &2);
        err.add_param(::std::borrow::Cow::from("max"), &8000);
        err.add_param(::std::borrow::Cow::from("value"), &self.token_url);

        let mut errors = ValidationErrors::new();
        errors.add("token_url", err);
        Err(errors)
    }
}
