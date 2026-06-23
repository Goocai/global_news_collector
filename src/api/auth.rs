use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    Router,
    routing::post,
};
use serde::{Deserialize,Serialize};
use sqlx::PgPool;
use bcrypt::{hash,verify,DEFAULT_COST};

use crate::auth::{encode_jwt,Claims};

#[derive(Deserialize)]
pub struct RegisterRequest {
    name:String,
    password:String,
    role:Option<String>,// 可选，默认为 human_expert
}

#[derive(Deserialize)]
pub struct LoginRequest {
    name: String,
    password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    token: String,
    user_id: i32,
    role: String,
}

pub fn routes()->Router<PgPool>{
    Router::new()
        .route("/register", post(register))
        .route("/login", post(login))
}

async fn register(
    State(pool): State<PgPool>,
    Json(payload): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, StatusCode> {
    if payload.name.is_empty() || payload.password.len() < 6 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let role = payload.role.unwrap_or_else(|| "human_expert".to_string());
    if role != "human_expert" && role != "admin" {
        return Err(StatusCode::BAD_REQUEST);
    }
    let hashed = hash(payload.password, DEFAULT_COST).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let row = sqlx::query!(
        "INSERT INTO users (name, password_hash, role) VALUES ($1, $2, $3) RETURNING id, role",
        payload.name,
        hashed,
        role
    )
    .fetch_one(&pool)
    .await
    .map_err(|_| StatusCode::CONFLICT)?; // 用户名重复

    let claims = Claims::new(row.id, &row.role);
    let token = encode_jwt(&claims).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(AuthResponse {
        token,
        user_id: row.id,
        role: row.role,
    }))
}

async fn login(
    State(pool): State<PgPool>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, StatusCode> {
    let row = sqlx::query!(
        "SELECT id, password_hash, role FROM users WHERE name = $1",
        payload.name
    )
    .fetch_optional(&pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    .ok_or(StatusCode::UNAUTHORIZED)?;

    let valid = verify(payload.password, &row.password_hash).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !valid {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let claims = Claims::new(row.id, &row.role);
    let token = encode_jwt(&claims).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(AuthResponse {
        token,
        user_id: row.id,
        role: row.role,
    }))
}
