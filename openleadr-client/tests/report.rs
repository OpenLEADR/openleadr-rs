use openleadr_client::{BusinessLogic, VirtualEndNode};
use openleadr_wire::{
    event::{EventInterval, EventType, EventValuesMap},
    program::ProgramRequest,
    values_map::Value,
};
use sqlx::PgPool;

mod common;
use common::AuthRole;

fn default_program() -> ProgramRequest {
    ProgramRequest {
        program_name: "report-test-program".to_string(),
        interval_period: None,
        program_descriptions: None,
        payload_descriptors: None,
        attributes: None,
        targets: vec![],
    }
}

#[sqlx::test(fixtures("users"))]
async fn report_crud(db: PgPool) {
    let bl = common::setup_client_with_role::<BusinessLogic>(db.clone(), AuthRole::Bl).await;
    let ven = common::setup_client_with_role::<VirtualEndNode>(db, AuthRole::Ven).await;

    let program = bl.create_program(default_program()).await.unwrap();
    let intervals = vec![EventInterval {
        id: 0,
        interval_period: None,
        payloads: vec![EventValuesMap {
            value_type: EventType::Price,
            values: vec![Value::Number(123.4)],
        }],
    }];
    let event_request = program.new_event(intervals);
    let bl_event = program.create_event(event_request).await.unwrap();

    // The BL `EventClient` can't create reports (no write_reports). Re-fetch the same event
    // through the VEN client so the ReportClient is bound to VEN credentials.
    let event = ven.get_event_by_id(bl_event.id()).await.unwrap();

    // Create
    let report_request_payload = event.new_report("ven-client".to_string());
    let mut report = event
        .create_report(report_request_payload.clone())
        .await
        .unwrap();

    assert_eq!(report.content(), &report_request_payload);

    // Get
    let listed = event.get_report_list(None).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id(), report.id());

    // Update
    let before = *report.modification_date_time();
    report.content_mut().report_name = Some("renamed".to_string());
    report.update().await.unwrap();
    assert_eq!(report.content().report_name.as_deref(), Some("renamed"));
    assert!(report.modification_date_time() > &before); // server bumps the timestamp

    // Delete
    report.delete().await.unwrap();
    let after_delete = event.get_report_list(None).await.unwrap();
    assert!(after_delete.is_empty());
}
