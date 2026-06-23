use axum::{
    extract::FromRequestParts,
    http::request::Parts,
    http::StatusCode,
};
use crate::auth::Claims;

pub struct AuthUser {
    pub user_id: i32,
    pub role: String,
}

impl<S> FromRequestParts<S> for AuthUser
where
    S: Send + Sync,
{
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let claims = parts.extensions
            .get::<Claims>()
            .cloned()                          
            .ok_or(StatusCode::UNAUTHORIZED)?;
        Ok(AuthUser {
            user_id: claims.sub,
            role: claims.role,
        })
    }
}