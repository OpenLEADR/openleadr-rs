use std::time::Duration;

use crate::common::AuthRole;
use openleadr_client::{BusinessLogic, MqttNotifications, VirtualEndNode};
use openleadr_wire::{
    program::{ProgramId, ProgramRequest},
    subscription::{AnyObject, Notification, Operation},
};
use sqlx::PgPool;

mod common;

async fn next_notification_for_program(
    notifications: &mut MqttNotifications,
    program_id: &ProgramId,
    timeout: Duration,
) -> Option<Notification> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let received = match tokio::time::timeout_at(deadline, notifications.recv()).await {
            Ok(Some(received)) => received.expect("failed to parse notification"),
            Ok(None) => panic!("notification stream closed"),
            Err(_) => return None,
        };
        let is_for_program =
            matches!(&received.object, AnyObject::Program(program) if &program.id == program_id);
        if is_for_program {
            return Some(received);
        }
    }
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_subscriptions(db: PgPool) {
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;
    let mut mqtt_client = ven.connect_mqtt().await.unwrap();
    mqtt_client.subscribe_events(None).await.unwrap();
    mqtt_client
        .subscribe_ven_programs("ven-id".parse().unwrap(), Some(Operation::Create))
        .await
        .unwrap();
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_receive_notification(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mut mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut notifications = mqtt_client.notifications();
    mqtt_client.subscribe_programs(None).await.unwrap();

    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
        .await
        .unwrap();

    let notification =
        next_notification_for_program(&mut notifications, program.id(), Duration::from_secs(5))
            .await
            .expect("no notification received in time");

    assert_eq!(notification.operation, Operation::Create);
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_unsubscribe(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mut mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut notifications = mqtt_client.notifications();
    let subscription = mqtt_client.subscribe_programs(None).await.unwrap();

    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
        .await
        .unwrap();

    let notification =
        next_notification_for_program(&mut notifications, program.id(), Duration::from_secs(5))
            .await
            .expect("no notification received in time");

    assert_eq!(notification.operation, Operation::Create);

    mqtt_client.unsubscribe(&subscription).await.unwrap();

    let unsubscribed_program = bl
        .create_program(ProgramRequest::new("mqtt-test-unsubscribe".to_string()))
        .await
        .unwrap();

    assert!(
        next_notification_for_program(
            &mut notifications,
            unsubscribed_program.id(),
            Duration::from_secs(1),
        )
        .await
        .is_none(),
        "should not receive a notification of this after unsubscribing"
    )
}
