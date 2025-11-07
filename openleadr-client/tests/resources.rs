use crate::common::setup;
use openleadr_vtn::jwt::AuthRole;
use openleadr_wire::{
    resource::ResourceContent,
    target::Target,
    values_map::{Value, ValueType, ValuesMap},
    ven::VenContent,
};
use serial_test::serial;
use std::str::FromStr;

mod common;

#[tokio::test]
#[serial]
async fn crud() {
    let ctx = setup(AuthRole::VenManager).await;

    // create new VEN
    let new = VenContent::new("ven-test".to_string(), None, vec![], None);
    let ven = ctx.create_ven(new).await.unwrap();

    // Create
    let new = ResourceContent {
        resource_name: "test-resource".to_string(),
        attributes: None,
        targets: vec![],
    };
    let created_resource = ven.create_resource(new.clone()).await.unwrap();
    assert_eq!(created_resource.content().resource_name, "test-resource");

    // Create with the same name fails for the same ven
    {
        let err = ven.create_resource(new.clone()).await.unwrap_err();
        assert!(err.is_conflict());
    }

    // Create with the same name succeeds for a different ven
    {
        let new_ven2 = VenContent::new("ven-test2".to_string(), None, vec![], None);
        let ven2 = ctx.create_ven(new_ven2).await.unwrap();

        let resource = ven2.create_resource(new).await.unwrap();

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
            .create_resource(ResourceContent {
                resource_name: "test-resource2".to_string(),
                attributes: None,
                targets: vec![],
            })
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
