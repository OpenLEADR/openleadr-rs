use crate::common::{setup, AuthRole, TestContext};
use openleadr_client::{BusinessLogic, Filter};
use openleadr_wire::{
    target::Target,
    values_map::{Value, ValueType, ValuesMap},
    ven::BlVenRequest,
    ClientId,
};
use serial_test::serial;
use std::str::FromStr;

mod common;

#[tokio::test]
#[serial]
async fn ven_crud() {
    let ctx: TestContext<BusinessLogic> = setup(AuthRole::Bl).await;

    // cleanup potentially clashing VEN
    {
        if let Ok(old_ven) = ctx.get_ven_by_name("crud-test-ven").await {
            assert_eq!(old_ven.content().ven_name, "crud-test-ven");
            old_ven.delete().await.unwrap();
        }
    }

    // Create
    let ven = BlVenRequest {
        client_id: "crud-test-ven-client-id".parse().unwrap(),
        targets: vec![],
        ven_name: "crud-test-ven".to_string(),
        attributes: None,
    };
    let create_ven = ctx.create_ven(ven.clone()).await.unwrap();
    assert_eq!(create_ven.content().ven_name, "crud-test-ven");

    // Create with the same name fails
    {
        let err = ctx.create_ven(ven).await.unwrap_err();
        assert!(err.is_conflict());
    }

    // Retrieve all
    {
        let vens = ctx.get_ven_list(Filter::none()).await.unwrap();
        assert!(vens.iter().any(|v| v.content().ven_name == "crud-test-ven"));
    }

    // Retrieve one by ID
    {
        let get_ven_id = ctx.get_ven_by_id(create_ven.id()).await.unwrap();
        assert_eq!(get_ven_id.content(), create_ven.content());
    }

    // Retrieve one by name
    let mut get_ven = ctx.get_ven_by_name("crud-test-ven").await.unwrap();
    assert_eq!(get_ven.content(), create_ven.content());
    assert_eq!(get_ven.content().ven_name, "crud-test-ven");

    // Update
    {
        let updated_name = "ven-test-update".to_string();
        let updated_attributes = Some(vec![ValuesMap {
            value_type: ValueType("PRICE".to_string()),
            values: vec![Value::Number(123.12)],
        }]);
        let updated_targets = vec![Target::from_str("group-1").unwrap()];

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

#[tokio::test]
#[serial]
async fn resource_crud() {
    let ctx = setup::<BusinessLogic>(AuthRole::Bl).await;

    // create new VEN
    let client_id: ClientId = "resource-ven-test-client-id".parse().unwrap();
    let new = BlVenRequest {
        client_id: client_id.clone(),
        targets: vec![],
        ven_name: "resource-ven-test".to_string(),
        attributes: None,
    };
    let ven = ctx.create_ven(new).await.unwrap();

    // Create
    let created_resource = ven
        .create_resource("test-resource", None, client_id.clone(), vec![])
        .await
        .unwrap();
    assert_eq!(created_resource.content().resource_name, "test-resource");

    // Create with the same name fails for the same ven
    {
        let err = ven
            .create_resource("test-resource", None, client_id.clone(), vec![])
            .await
            .unwrap_err();
        assert!(err.is_conflict());
    }

    // Create with the same name succeeds for a different ven
    {
        let client_id: ClientId = "resource-ven-2-test-client-id".parse().unwrap();
        let new_ven2 = BlVenRequest {
            client_id: client_id.clone(),
            targets: vec![],
            ven_name: "resource-ven-2-test".to_string(),
            attributes: None,
        };
        let ven2 = ctx.create_ven(new_ven2).await.unwrap();

        let resource = ven2
            .create_resource("test-resource", None, client_id, vec![])
            .await
            .unwrap();

        // Cleanup
        resource.delete().await.unwrap();
        ven2.delete().await.unwrap();
    }

    // Retrieve all
    {
        let resources = ven.get_all_resources(None).await.unwrap();
        assert!(resources
            .iter()
            .any(|r| r.content().resource_name == "test-resource"));
    }

    // Retrieve one by name
    {
        let resource2 = ven
            .create_resource("test-resource2".to_string(), None, client_id, vec![])
            .await
            .unwrap();
        let get_resource = ven.get_resource_by_name("test-resource").await.unwrap();
        assert_eq!(get_resource.content(), created_resource.content());
        resource2.delete().await.unwrap();
    }

    // Retrieve one by ID
    let mut get_resource = ven.get_resource_by_id(created_resource.id()).await.unwrap();
    assert_eq!(get_resource.content(), created_resource.content());

    // Update
    {
        let updated_name = "test-resource-updated".to_string();
        let updated_attributes = Some(vec![ValuesMap {
            value_type: ValueType("PRICE".to_string()),
            values: vec![Value::Number(123.12)],
        }]);
        let updated_targets = vec![Target::from_str("group-1").unwrap()];

        get_resource.content_mut().resource_name = updated_name.clone();
        get_resource.content_mut().attributes = updated_attributes.clone();
        get_resource.content_mut().targets = updated_targets.clone();
        get_resource.update().await.unwrap();

        assert_eq!(get_resource.content().resource_name, updated_name);
        assert_eq!(get_resource.content().attributes, updated_attributes);
        assert_eq!(get_resource.content().targets, updated_targets);

        let get_resource2 = ven
            .get_resource_by_name("test-resource-updated")
            .await
            .unwrap();
        assert_eq!(get_resource2.content().resource_name, updated_name);
        assert_eq!(get_resource2.content().attributes, updated_attributes);
        assert_eq!(get_resource2.content().targets, updated_targets);

        assert_eq!(
            get_resource2.modification_date_time(),
            get_resource.modification_date_time()
        );
        assert_ne!(
            created_resource.modification_date_time(),
            get_resource.modification_date_time()
        );
        assert_eq!(
            get_resource2.created_date_time(),
            created_resource.created_date_time()
        )
    }

    // Delete
    {
        let id = created_resource.id().clone();
        created_resource.delete().await.unwrap();
        let err = ven.get_resource_by_id(&id).await.unwrap_err();
        assert!(err.is_not_found())
    }

    // Cleanup
    {
        ven.delete().await.unwrap();
    }
}
