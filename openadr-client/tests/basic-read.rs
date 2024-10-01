use openadr_wire::program::ProgramContent;
use sqlx::PgPool;

mod common;

#[sqlx::test(fixtures("users"))]
async fn basic_create_read(db: PgPool) -> Result<(), openadr_client::Error> {
    let client = common::setup_client(db).await;

    client
        .create_program(ProgramContent::new("test-prog"))
        .await?;

    let programs = client.get_all_programs().await?;
    assert_eq!(programs.len(), 1);
    assert_eq!(programs[0].content().program_name, "test-prog");

    Ok(())
}
