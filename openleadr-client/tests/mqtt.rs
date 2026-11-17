use std::time::Duration;

use crate::common::AuthRole;
use openleadr_client::{BusinessLogic, MqttSubscription, VirtualEndNode};
use openleadr_wire::{
    program::{ProgramId, ProgramRequest},
    subscription::{AnyObject, Notification, Operation},
};
use sqlx::PgPool;

mod common;

async fn next_notification_for_program(
    subscription: &mut MqttSubscription,
    program_id: &ProgramId,
    timeout: Duration,
) -> Option<Notification> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let received = match tokio::time::timeout_at(deadline, subscription.recv()).await {
            Ok(received) => received.expect("failed to receive notification"),
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
async fn mqtt_receive_notification(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut subscription = mqtt_client.subscribe_programs(None).await.unwrap();

    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
        .await
        .unwrap();

    let notification =
        next_notification_for_program(&mut subscription, program.id(), Duration::from_secs(5))
            .await
            .expect("no notification received in time");

    assert_eq!(notification.operation, Operation::Create);
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_subscription_survives_dropped_clone(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut subscription = mqtt_client.subscribe_programs(None).await.unwrap();
    {
        let mut clone = subscription.clone();

        let program = bl
            .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
            .await
            .unwrap();

        // Both receive every notification.
        for subscription in [&mut subscription, &mut clone] {
            next_notification_for_program(subscription, program.id(), Duration::from_secs(5))
                .await
                .expect("no notification received in time");
        }
    }

    // Dropping the clone must not unsubscribe the original.
    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-after-drop".to_string()))
        .await
        .unwrap();

    next_notification_for_program(&mut subscription, program.id(), Duration::from_secs(5))
        .await
        .expect("no notification received in time");
}

#[sqlx::test(fixtures("users"))]
async fn mqtt_subscriptions_receive_only_their_topic(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let mqtt_client = ven.connect_mqtt().await.unwrap();
    let mut creates = mqtt_client
        .subscribe_programs(Some(Operation::Create))
        .await
        .unwrap();
    let mut deletes = mqtt_client
        .subscribe_programs(Some(Operation::Delete))
        .await
        .unwrap();

    let program = bl
        .create_program(ProgramRequest::new("mqtt-test-program".to_string()))
        .await
        .unwrap();

    let notification =
        next_notification_for_program(&mut creates, program.id(), Duration::from_secs(5))
            .await
            .expect("no notification received in time");
    assert_eq!(notification.operation, Operation::Create);

    assert!(
        next_notification_for_program(&mut deletes, program.id(), Duration::from_secs(1))
            .await
            .is_none(),
        "the delete subscription should not receive the create notification"
    );
}
