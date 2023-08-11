use std::{env::var, ops::DerefMut};

use actix_web::{web, App, HttpServer};
use deadpool_postgres::{tokio_postgres::Config, Manager, ManagerConfig, Pool, RecyclingMethod};
use handlers::{count_people, create_person, get_person, search_people};
use tokio_postgres::NoTls;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

pub mod models {
    use chrono::NaiveDate;
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    pub struct PersonInput {
        pub apelido: String,
        pub nome: String,
        pub nascimento: NaiveDate,
        pub stack: Option<Vec<String>>,
    }

    #[derive(Serialize)]
    pub struct PersonOutput {
        pub id: String,
        pub apelido: String,
        pub nome: String,
        pub nascimento: NaiveDate,
        pub stack: Option<Vec<String>>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Q {
        pub t: String,
    }
}

pub mod handlers {
    use actix_web::{web, Error, HttpRequest, HttpResponse};
    use deadpool_postgres::{Client, Pool};
    use futures::StreamExt;
    use uuid::Uuid;

    use crate::models::{PersonInput, PersonOutput, Q};

    pub async fn create_person(
        req: HttpRequest,
        mut payload: web::Payload,
    ) -> Result<HttpResponse, Error> {
        // Deserialize body
        let mut body = web::BytesMut::new();
        while let Some(chunk) = payload.next().await {
            let chunk = chunk?;
            body.extend_from_slice(&chunk);
        }

        let Ok(p) = serde_json::from_slice::<PersonInput>(&body) else {
            return Err(actix_web::error::ErrorUnprocessableEntity("Unprocessable Entity"));
        };

        let pool = req.app_data::<web::Data<Pool>>().unwrap();
        let client: Client = pool.get().await.unwrap();

        let stmt = client
            .prepare_cached("insert into people (id, apelido, nome, nascimento, stack) values ($1, $2, $3, $4, $5)")
            .await.unwrap();

        let id = Uuid::new_v4();

        if let Err(e) = client
            .query(&stmt, &[&id, &p.apelido, &p.nome, &p.nascimento, &p.stack])
            .await
        {
            return match e.code() {
                Some(code) => match code {
                    &tokio_postgres::error::SqlState::UNIQUE_VIOLATION => Err(
                        actix_web::error::ErrorUnprocessableEntity("Unprocessable Entity"),
                    ),
                    &tokio_postgres::error::SqlState::INTEGRITY_CONSTRAINT_VIOLATION => Err(
                        actix_web::error::ErrorUnprocessableEntity("Unprocessable Entity"),
                    ),
                    _ => Err(actix_web::error::ErrorInternalServerError(
                        "Internal Server Error",
                    )),
                },
                None => Err(actix_web::error::ErrorInternalServerError(
                    "Internal Server Error",
                )),
            };
        }

        Ok(HttpResponse::Created()
            .insert_header(("Location", format!("/pessoas/{id}")))
            .finish())
    }

    pub async fn get_person(id: web::Path<String>, db_pool: web::Data<Pool>) -> HttpResponse {
        let client: Client = db_pool.get().await.unwrap();
        let person_id = Uuid::parse_str(id.into_inner().as_str()).unwrap();

        let stmt = client
            .prepare_cached("select id, apelido, nome, nascimento, stack from people where id = $1")
            .await
            .unwrap();

        let opt_row = client.query_opt(&stmt, &[&person_id]).await.unwrap();

        match opt_row {
            Some(row) => {
                let person_id: Uuid = row.get("id");
                let person = PersonOutput {
                    id: person_id.to_string(),
                    apelido: row.get("apelido"),
                    nome: row.get("nome"),
                    nascimento: row.get("nascimento"),
                    stack: row.get("stack"),
                };
                HttpResponse::Ok().json(person)
            }
            None => HttpResponse::NotFound().finish(),
        }
    }

    pub async fn search_people(query: web::Query<Q>, db_pool: web::Data<Pool>) -> HttpResponse {
        let client: Client = db_pool.get().await.unwrap();

        let stmt = client
            .prepare_cached("select id, apelido, nome, nascimento, stack from people where for_search like ('%' || $1 || '%') ")
            .await
            .unwrap();

        let rows = client.query(&stmt, &[&query.t]).await.unwrap();

        let people: Vec<PersonOutput> = rows
            .iter()
            .map(|row| {
                let person_id: Uuid = row.get("id");
                PersonOutput {
                    id: person_id.to_string(),
                    apelido: row.get("apelido"),
                    nome: row.get("nome"),
                    nascimento: row.get("nascimento"),
                    stack: row.get("stack"),
                }
            })
            .collect();

        HttpResponse::Ok().json(people)
    }

    pub async fn count_people(db_pool: web::Data<Pool>) -> HttpResponse {
        let client: Client = db_pool.get().await.unwrap();
        let stmt = client
            .prepare_cached("select count(1)::TEXT from people")
            .await
            .unwrap();

        let row = client.query_one(&stmt, &[]).await.unwrap();
        let count: String = row.get(0);

        HttpResponse::Ok().body(count)
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // get database pool
    let pg_config = Config::new()
        .host(&var("DB_HOST").unwrap_or("localhost".into()))
        .user(&var("DB_USER").unwrap_or("postgres".into()))
        .dbname(&var("DB_NAME").unwrap_or("postgres".into()))
        .password(&var("DB_PASS").unwrap_or("password".into()))
        .to_owned();

    let manager_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let manager = Manager::from_config(pg_config, NoTls, manager_config);

    let pool = Pool::builder(manager).max_size(16).build().unwrap();

    // sync run migrations
    let mut conn = pool.get().await.unwrap();
    let client = conn.deref_mut().deref_mut();
    embedded::migrations::runner()
        .run_async(client)
        .await
        .unwrap();

    // configure server
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .route("/pessoas", web::post().to(create_person))
            .route("/pessoas/{id}", web::get().to(get_person))
            .route("/pessoas", web::get().to(search_people))
            .route("/contagem-pessoas", web::get().to(count_people))
    })
    .bind(("0.0.0.0", 80))?
    .run();

    println!("Will listen");

    // start server
    server.await
}
