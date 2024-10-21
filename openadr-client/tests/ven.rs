use crate::common::setup;
use openadr_client::Filter;
use openadr_vtn::jwt::AuthRole;
use openadr_wire::ven::VenContent;
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
#[ignore = "Filtering by ven_name depends on #21"]
async fn crud() {
    let ctx = setup(AuthRole::VenManager).await;
    let ven = VenContent::new("test-ven".to_string());
    let ven = ctx.create_ven(ven).await.unwrap();
    let vens = ctx
        .get_ven_list(Some("test-ven"), Filter::None)
        .await
        .unwrap();
    assert_eq!(vens.len(), 1);
    assert_eq!(vens[0].content().ven_name, "test-ven");

    ven.delete().await.unwrap();
}
