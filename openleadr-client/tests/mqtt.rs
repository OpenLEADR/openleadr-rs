use std::time::Duration;

use crate::common::AuthRole;
use openleadr_client::{BusinessLogic, MqttSubscriptionTarget, VirtualEndNode};
use openleadr_wire::{
    program::ProgramRequest,
    subscription::{AnyObject, Operation},
};
use sqlx::PgPool;

mod common;

#[sqlx::test(fixtures("users"))]
async fn mqtt_subscriptions(db: PgPool) {
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;
    let mqtt_client = ven.connect_mqtt().await.unwrap();
    mqtt_client
        .subscribe(MqttSubscriptionTarget::Events, None)
        .await
        .unwrap();
    mqtt_client
        .subscribe(
            MqttSubscriptionTarget::VenPrograms("ven-id".parse().unwrap()),
            Some(Operation::Create),
        )
        .await
        .unwrap();
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_receive_notification(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut notifications = mqtt_client.notifications();
    mqtt_client
        .subscribe(MqttSubscriptionTarget::Programs, None)
        .await
        .unwrap();

    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
        .await
        .unwrap();

    let notification = tokio::time::timeout(Duration::from_secs(5), notifications.recv())
        .await
        .expect("no notification received in time")
        .expect("notification stream closed")
        .unwrap();

    assert_eq!(notification.operation, Operation::Create);
    let AnyObject::Program(received) = notification.object else {
        panic!("expected a program, got {:?}", notification.object);
    };
    assert_eq!(&received.id, program.id());
}
