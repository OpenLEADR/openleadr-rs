use crate::common::setup;
use openleadr_client::Filter;
use openleadr_vtn::jwt::AuthRole;
use openleadr_wire::{
    target::{TargetEntry, TargetMap, TargetType},
    values_map::{Value, ValueType, ValuesMap},
    ven::VenContent,
};
use serial_test::serial;

mod common;

#[tokio::test]
#[serial]
async fn crud() {
    let ctx = setup(AuthRole::VenManager).await;

    // cleanup potentially clashing VEN
    {
        if let Ok(old_ven) = ctx.get_ven_by_name("ven-test").await {
            assert_eq!(old_ven.content().ven_name, "ven-test");
            old_ven.delete().await.unwrap();
        }
    }

    // Create
    let ven = VenContent::new("test-ven".to_string(), None, None, None);
    let create_ven = ctx.create_ven(ven.clone()).await.unwrap();
    assert_eq!(create_ven.content().ven_name, "test-ven");

    // Create with the same name fails
    {
        let err = ctx.create_ven(ven).await.unwrap_err();
        assert!(err.is_conflict());
    }

    // Retrieve all
    {
        let vens = ctx.get_ven_list(Filter::none()).await.unwrap();
        assert!(vens.iter().any(|v| v.content().ven_name == "test-ven"));
    }

    // Retrieve one by ID
    {
        let get_ven_id = ctx.get_ven_by_id(create_ven.id()).await.unwrap();
        assert_eq!(get_ven_id.content(), create_ven.content());
    }

    // Retrieve one by name
    let mut get_ven = ctx.get_ven_by_name("test-ven").await.unwrap();
    assert_eq!(get_ven.content(), create_ven.content());
    assert_eq!(get_ven.content().ven_name, "test-ven");

    // Update
    {
        let updated_name = "ven-test-update".to_string();
        let updated_attributes = Some(vec![ValuesMap {
            value_type: ValueType("PRICE".to_string()),
            values: vec![Value::Number(123.12)],
        }]);
        let updated_targets = Some(TargetMap(vec![TargetEntry {
            label: TargetType::Group,
            values: vec!["group-1".to_string()],
        }]));

        get_ven.content_mut().ven_name = updated_name.clone();
        get_ven.content_mut().attributes = updated_attributes.clone();
        get_ven.content_mut().targets = updated_targets.clone();
        get_ven.update().await.unwrap();

        assert_eq!(get_ven.content().ven_name, updated_name);
        assert_eq!(get_ven.content().attributes, updated_attributes);
        assert_eq!(get_ven.content().targets, updated_targets);

        let get_ven2 = ctx.get_ven_by_name("ven-test-update").await.unwrap();
        assert_eq!(get_ven2.content().ven_name, updated_name);
        assert_eq!(get_ven2.content().attributes, updated_attributes);
        assert_eq!(get_ven2.content().targets, updated_targets);

        assert_eq!(
            get_ven2.modification_date_time(),
            get_ven.modification_date_time()
        );
        assert_ne!(
            create_ven.modification_date_time(),
            get_ven.modification_date_time()
        );
        assert_eq!(get_ven2.created_date_time(), create_ven.created_date_time())
    }

    // Delete
    get_ven.delete().await.unwrap();
}
