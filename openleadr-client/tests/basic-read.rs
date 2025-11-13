use openleadr_client::{Client, Filter, VirtualEndNode};
use openleadr_wire::program::ProgramRequest;
use sqlx::PgPool;

mod common;

#[sqlx::test(fixtures("users"))]
async fn basic_create_read(db: PgPool) -> Result<(), openleadr_client::Error> {
    let client: Client<VirtualEndNode> = common::setup_client(db).await;

    client
        .create_program(ProgramRequest::new("test-prog"))
        .await?;

    let programs = client.get_program_list(Filter::none()).await?;
    assert_eq!(programs.len(), 1);
    assert_eq!(programs[0].content().program_name, "test-prog");

    Ok(())
}
