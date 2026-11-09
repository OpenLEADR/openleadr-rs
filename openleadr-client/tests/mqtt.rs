use crate::common::AuthRole;
use openleadr_client::{MqttSubscriptionTarget, VirtualEndNode};
use openleadr_wire::subscription::Operation;
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
