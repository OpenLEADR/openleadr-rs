use reqwest::StatusCode;

/// Errors that can occur using the [`Client`](crate::Client)
#[derive(Debug)]
#[allow(missing_docs)]
pub enum Error {
    Reqwest(reqwest::Error),
    Serde(serde_json::Error),
    UrlParseError(url::ParseError),
    Problem(openleadr_wire::problem::Problem),
    AuthProblem(openleadr_wire::oauth::OAuthError),
    OAuthTokenNotBearer,
    ObjectNotFound,
    DuplicateObject,
    /// Error if you try
    /// to create an event underneath a program
    /// where the [`program_id`](crate::EventContent::program_id)
    /// in the [`EventContent`](crate::EventContent)
    /// does not match the program ID of the [`ProgramClient`](crate::ProgramClient),
    /// for example.
    InvalidParentObject,
    InvalidInterval,
}

impl Error {
    /// Checks if the [`Problem`](openleadr_wire::problem::Problem) response of the VTN is a
    /// `409 Conflict` HTTP status code.
    pub fn is_conflict(&self) -> bool {
        match self {
            Error::Problem(openleadr_wire::problem::Problem { status, .. }) => {
                *status == StatusCode::CONFLICT
            }
            _ => false,
        }
    }

    #[allow(missing_docs)]
    pub fn is_not_found(&self) -> bool {
        match self {
            Error::Problem(openleadr_wire::problem::Problem { status, .. }) => {
                *status == StatusCode::NOT_FOUND
            }
            _ => false,
        }
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::Reqwest(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serde(err)
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::UrlParseError(err)
    }
}

impl From<openleadr_wire::problem::Problem> for Error {
    fn from(err: openleadr_wire::problem::Problem) -> Self {
        Error::Problem(err)
    }
}

impl From<openleadr_wire::oauth::OAuthError> for Error {
    fn from(err: openleadr_wire::oauth::OAuthError) -> Self {
        Error::AuthProblem(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Reqwest(err) => write!(f, "Reqwest error: {}", err),
            Error::Serde(err) => write!(f, "Serde error: {}", err),
            Error::UrlParseError(err) => write!(f, "URL parse error: {}", err),
            Error::Problem(err) => write!(f, "OpenADR Problem: {:?}", err),
            Error::AuthProblem(err) => write!(f, "Authentication problem: {:?}", err),
            Error::ObjectNotFound => write!(f, "Object not found"),
            Error::DuplicateObject => write!(f, "Found more than one object matching the filter"),
            Error::InvalidParentObject => write!(f, "Invalid parent object"),
            Error::InvalidInterval => write!(f, "Invalid interval specified"),
            Error::OAuthTokenNotBearer => write!(f, "OAuth token received is not a Bearer token"),
        }
    }
}

impl std::error::Error for Error {}

pub(crate) type Result<T> = std::result::Result<T, Error>;
