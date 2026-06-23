pub mod brier;
pub mod anti_fragile;

use axum::Router;
use sqlx::PgPool;

pub fn routers() -> Router<PgPool>{
    Router::new()
        .nest("/stats",brier::routes())
        .nest("/anti-fragile", anti_fragile::routes())
    
}

