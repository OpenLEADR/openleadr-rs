use openleadr_client::{BusinessLogic, Client, ClientKind, ResourceGroupClient};
use openleadr_wire::{
    resource::ResourceId,
    resource_group::{BlResourceGroupRequest, ResourceGroupChild},
    target::Target,
    ven::BlVenRequest,
};
use sqlx::PgPool;
use std::str::FromStr;

use crate::common::AuthRole;

mod common;

fn content(name: &str, children: Vec<ResourceGroupChild>) -> BlResourceGroupRequest {
    BlResourceGroupRequest {
        targets: vec![],
        resource_group_name: name.to_string(),
        attributes: None,
        children,
    }
}

fn group_child<K: ClientKind>(group: &ResourceGroupClient<K>) -> ResourceGroupChild {
    ResourceGroupChild::ResourceGroup(group.id().clone())
}

fn resource_child(id: &ResourceId) -> ResourceGroupChild {
    ResourceGroupChild::VenResource(id.clone())
}

// The VTN does not guarantee the order of the children.
fn assert_same_children(actual: &[ResourceGroupChild], expected: &[ResourceGroupChild]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{actual:?} differs from {expected:?}"
    );
    for child in expected {
        assert!(actual.contains(child), "{child:?} is missing in {actual:?}");
    }
}

/// Creates a VEN with a single resource as the business logic.
async fn bl_resource(client: &Client<BusinessLogic>, name: &str) -> ResourceId {
    let ven = client
        .create_ven(BlVenRequest {
            client_id: format!("{name}-client-id").parse().unwrap(),
            targets: vec![],
            ven_name: format!("{name}-ven"),
            attributes: None,
        })
        .await
        .unwrap();

    ven.create_resource(name, None, vec![])
        .await
        .unwrap()
        .id()
        .clone()
}

#[sqlx::test(fixtures("users"))]
async fn bl_crud(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db, AuthRole::Bl).await;
    let resource = bl_resource(&bl, "resource").await;

    // Create
    let child = bl
        .create_resource_group(content("nested", vec![]))
        .await
        .unwrap();
    let request = content(
        "resource-group",
        vec![resource_child(&resource), group_child(&child)],
    );
    let created = bl.create_resource_group(request.clone()).await.unwrap();
    assert_eq!(created.content().resource_group_name, "resource-group");
    assert_same_children(&created.content().children, &request.children);

    // Names do not have to be unique
    {
        let duplicate = bl.create_resource_group(request.clone()).await.unwrap();
        assert_ne!(duplicate.id(), created.id());
        duplicate.delete().await.unwrap();
    }

    // Children must exist
    {
        let err = bl
            .create_resource_group(content(
                "unknown-child",
                vec![ResourceGroupChild::VenResource(
                    "does-not-exist".parse().unwrap(),
                )],
            ))
            .await
            .unwrap_err();
        assert!(err.is_conflict(), "{err:?}");
    }

    // Retrieve all
    {
        let all = bl.get_all_resource_groups(None).await.unwrap();
        assert!(all.iter().any(|rg| rg.id() == created.id()));
        assert!(all.iter().any(|rg| rg.id() == child.id()));
    }

    // Retrieve all by name
    {
        let by_name = bl
            .get_all_resource_groups(Some("resource-group"))
            .await
            .unwrap();
        assert_eq!(by_name.len(), 1);
        assert_eq!(by_name[0].id(), created.id());
    }

    // Retrieve one by ID
    let mut retrieved = bl.get_resource_group_by_id(created.id()).await.unwrap();
    assert_eq!(retrieved.content().resource_group_name, "resource-group");
    assert_same_children(&retrieved.content().children, &request.children);

    // Update
    {
        let other_resource = bl_resource(&bl, "other-resource").await;
        let updated_targets = vec![Target::from_str("group-1").unwrap()];
        // Replaces the old children
        let updated_children = vec![resource_child(&other_resource)];

        let content = retrieved.content_mut();
        content.resource_group_name = "resource-group-updated".to_string();
        content.targets = updated_targets.clone();
        content.children = updated_children.clone();
        retrieved.update().await.unwrap();

        let refetched = bl.get_resource_group_by_id(created.id()).await.unwrap();
        for rg in [&retrieved, &refetched] {
            assert_eq!(rg.content().resource_group_name, "resource-group-updated");
            assert_eq!(rg.content().targets, updated_targets);
            assert_same_children(&rg.content().children, &updated_children);
        }

        assert_eq!(refetched.created_date_time(), created.created_date_time());
        assert_eq!(
            refetched.modification_date_time(),
            retrieved.modification_date_time()
        );
        assert_ne!(
            created.modification_date_time(),
            retrieved.modification_date_time()
        );
    }

    // Delete
    {
        let id = created.id().clone();
        let deleted = retrieved.delete().await.unwrap();
        assert_eq!(deleted.id, id);

        let err = bl.get_resource_group_by_id(&id).await.unwrap_err();
        assert!(err.is_not_found(), "{err:?}");

        // Deleting a parent leaves its children intact
        bl.get_resource_group_by_id(child.id()).await.unwrap();
    }
}
