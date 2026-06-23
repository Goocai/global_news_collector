use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use chrono::{Duration,Utc};
use std::env;
use std::sync::LazyLock;

pub mod middleware;
pub mod extractor;


static JWT_SECRET:LazyLock<Vec<u8>> = LazyLock::new(|| {
    env::var("JWT_SECRET")
        .expect("JWT_SECRET must be set!")
        .into_bytes()  
});

#[derive(Debug,Serialize,Deserialize,Clone)]
pub struct Claims{
    pub sub:i32,    //user_id
    pub exp:usize,  //过期时间 （时间戳）
    pub role:String,    //用户角色
}

impl Claims{
    pub fn new(user_id:i32,role:&str) -> Self{
        let exp = (Utc::now()+ Duration::hours(24)).timestamp() as usize;
        Self { 
                sub: user_id,
                exp, 
                role: role.to_string(),
        }
    }
}

/// 生成 JWT
pub fn encode_jwt(claims: &Claims ) -> Result<String, Box<dyn std::error::Error + Send + Sync>>{
    let handler = Header::default();
    let token = encode(&handler, claims, &EncodingKey::from_secret(&JWT_SECRET))?;
    Ok(token)
}   

/// 验证 JWT，返回 Claims
pub fn decode_jwt(token: &str) -> Result<Claims, Box<dyn std::error::Error +Send + Sync>> {
    let validation = Validation::default();
    let token_data = decode::<Claims>(token, &DecodingKey::from_secret(&JWT_SECRET), &validation)?;
    Ok(token_data.claims)
}