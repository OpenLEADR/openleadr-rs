use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, trace};
use validator::Validate;

use openleadr_wire::{
    ClientId, Report,
    event::EventId,
    program::ProgramId,
    report::{ReportId, ReportRequest},
    subscription::{AnyObject, Operation},
};

use crate::{
    api::{
        AppResponse, ValidatedJson, ValidatedQuery,
        subscription::{self, NotifierState},
    },
    data_source::{EventCrud, ReportCrud, VenCrud, VenObjectPrivacy},
    error::AppError,
    jwt::{Scope, User},
};

#[instrument(skip(ven_source, event_source, privacy, report_source, notifier_state))]
pub async fn mqtt_add(
    ven_source: Arc<dyn VenCrud>,
    event_source: Arc<dyn EventCrud>,
    privacy: Arc<dyn VenObjectPrivacy>,
    report_source: Arc<dyn ReportCrud>,
    notifier_state: Arc<NotifierState>,
    client_id: ClientId,
    new_report: ReportRequest,
) -> Result<(), AppError> {
    let report = report_source
        .create(new_report, &Some(client_id.clone()))
        .await?;

    info!(%report.id, report_name=?report.content.report_name, client_id = client_id.to_string(), "report created via mqtt");

    subscription::notify(
        &*ven_source,
        &*event_source,
        &*privacy,
        &notifier_state,
        Operation::Create,
        AnyObject::Report(report.clone()),
    )
    .await;

    Ok(())
}

#[instrument(skip(user, report_source))]
pub async fn get_all(
    State(report_source): State<Arc<dyn ReportCrud>>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    User(user): User,
) -> AppResponse<Vec<Report>> {
    let reports = if user.has_scope(Scope::ReadAll) {
        report_source.retrieve_all(&query_params, &None).await?
    } else if user.has_scope(Scope::ReadVenObjects) {
        report_source
            .retrieve_all(&query_params, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(client_id = user.sub, "retrieved {} reports", reports.len());

    Ok(Json(reports))
}

#[instrument(skip(user, report_source))]
pub async fn get(
    State(report_source): State<Arc<dyn ReportCrud>>,
    Path(id): Path<ReportId>,
    User(user): User,
) -> AppResponse<Report> {
    let report = if user.has_scope(Scope::ReadAll) {
        report_source.retrieve(&id, &None).await?
    } else if user.has_scope(Scope::ReadVenObjects) {
        report_source
            .retrieve(&id, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'read_all' or 'read_ven_objects' scope",
        ));
    };

    trace!(%report.id, report.report_name=report.content.report_name, client_id = user.sub, "retrieved report");

    Ok(Json(report))
}

#[instrument(skip(user, ven_source, event_source, privacy, report_source, notifier_state))]
pub async fn add(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(event_source): State<Arc<dyn EventCrud>>,
    State(privacy): State<Arc<dyn VenObjectPrivacy>>,
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    ValidatedJson(new_report): ValidatedJson<ReportRequest>,
) -> Result<(StatusCode, Json<Report>), AppError> {
    let report = if user.has_scope(Scope::WriteReportsBl) || user.has_scope(Scope::WriteReportsVen)
    {
        report_source
            .create(new_report, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'write_reports_bl' or 'write_reports_ven' scope",
        ));
    };

    info!(%report.id, report_name=?report.content.report_name, client_id = user.sub, "report created");

    subscription::notify(
        &*ven_source,
        &*event_source,
        &*privacy,
        &notifier_state,
        Operation::Create,
        AnyObject::Report(report.clone()),
    )
    .await;

    Ok((StatusCode::CREATED, Json(report)))
}

#[expect(
    clippy::too_many_arguments,
    reason = "Handler which uses many aspects of the state"
)]
#[instrument(skip(user, ven_source, event_source, privacy, report_source, notifier_state))]
pub async fn edit(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(event_source): State<Arc<dyn EventCrud>>,
    State(privacy): State<Arc<dyn VenObjectPrivacy>>,
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    Path(id): Path<ReportId>,
    User(user): User,
    ValidatedJson(content): ValidatedJson<ReportRequest>,
) -> AppResponse<Report> {
    let report = if user.has_scope(Scope::WriteReportsBl) {
        report_source.update(&id, content, &None).await?
    } else if user.has_scope(Scope::WriteReportsVen) {
        report_source
            .update(&id, content, &Some(user.client_id()?))
            .await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'write_reports_bl' or 'write_reports_ven' scope",
        ));
    };

    info!(%report.id, report_name=?report.content.report_name, client_id = user.sub, "report updated");

    subscription::notify(
        &*ven_source,
        &*event_source,
        &*privacy,
        &notifier_state,
        Operation::Update,
        AnyObject::Report(report.clone()),
    )
    .await;

    Ok(Json(report))
}

#[instrument(skip(user, ven_source, event_source, privacy, report_source, notifier_state))]
pub async fn delete(
    State(ven_source): State<Arc<dyn VenCrud>>,
    State(event_source): State<Arc<dyn EventCrud>>,
    State(privacy): State<Arc<dyn VenObjectPrivacy>>,
    State(report_source): State<Arc<dyn ReportCrud>>,
    State(notifier_state): State<Arc<NotifierState>>,
    User(user): User,
    Path(id): Path<ReportId>,
) -> AppResponse<Report> {
    let report = if user.has_scope(Scope::WriteReportsBl) {
        report_source.delete(&id, &None).await?
    } else if user.has_scope(Scope::WriteReportsVen) {
        report_source.delete(&id, &Some(user.client_id()?)).await?
    } else {
        return Err(AppError::Forbidden(
            "Missing 'write_reports_bl' or 'write_reports_ven' scope",
        ));
    };

    info!(%id, report_name=?report.content.report_name, client_id = user.sub, "deleted report");

    subscription::notify(
        &*ven_source,
        &*event_source,
        &*privacy,
        &notifier_state,
        Operation::Delete,
        AnyObject::Report(report.clone()),
    )
    .await;

    Ok(Json(report))
}

#[derive(Serialize, Deserialize, Validate, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[serde(rename = "programID")]
    pub(crate) program_id: Option<ProgramId>,
    #[serde(rename = "eventID")]
    pub(crate) event_id: Option<EventId>,
    #[validate(length(min = 1, max = 128))]
    pub(crate) client_name: Option<String>,
    #[serde(default)]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

fn get_50() -> i64 {
    50
}

#[cfg(test)]
#[cfg(feature = "live-db-test")]
mod test {
    use std::{collections::BTreeSet, time::Duration};

    use crate::{api::test::ApiTest, jwt::Scope};
    use axum::{body::Body, http, http::StatusCode};
    use openleadr_wire::{
        ObjectType, Report,
        problem::Problem,
        report::{
            MqttUploadRequest, ReportPayloadDescriptor, ReportRequest, ReportType, UploadOperation,
        },
        subscription::{MqttPushNotification, NotifierTopicsResponse, Operation},
    };
    use paho_mqtt::{Message, QoS};
    use sqlx::PgPool;

    fn default() -> ReportRequest {
        ReportRequest {
            event_id: "asdf".parse().unwrap(),
            client_name: "".to_string(),
            report_name: None,
            payload_descriptors: None,
            resources: vec![],
        }
    }

    #[sqlx::test]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(
            db,
            "ven-1-client-id",
            vec![Scope::WriteVensVen, Scope::ReadTargets],
        )
        .await;

        let reports = [
            ReportRequest {
                report_name: Some("".to_string()),
                ..default()
            },
            ReportRequest {
                report_name: Some("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                ..default()
            },
            ReportRequest {
                payload_descriptors: Some(vec![
                    ReportPayloadDescriptor{
                        payload_type: ReportType::Private("".to_string()),
                        reading_type: Default::default(),
                        units: None,
                        accuracy: None,
                        confidence: None,
                    }
                ]),
                ..default()
            },
            ReportRequest {
                payload_descriptors: Some(vec![
                    ReportPayloadDescriptor{
                        payload_type: ReportType::Private("This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string()),
                        reading_type: Default::default(),
                        units: None,
                        accuracy: None,
                        confidence: None,
                    }
                ]),
                ..default()
            },
        ];

        for report in &reports {
            let (status, error) = test
                .request::<Problem>(
                    http::Method::POST,
                    "/reports",
                    Body::from(serde_json::to_vec(&report).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(
                error
                    .detail
                    .unwrap()
                    .contains("outside of allowed range 1..=128")
            )
        }
    }

    #[sqlx::test(fixtures("programs", "events", "vens"))]
    async fn bl_client_can_delete_ven_created_report(db: PgPool) {
        let ven = ApiTest::new(db.clone(), "ven-1-client-id", vec![Scope::WriteReportsVen]).await;
        let bl = ApiTest::new(db, "bl-client-id", vec![Scope::WriteReportsBl]).await;

        let (status, report) = ven
            .request::<Report>(
                http::Method::POST,
                "/reports",
                Body::from(
                    serde_json::to_vec(&ReportRequest {
                        event_id: "event-1".parse().unwrap(),
                        client_name: "ven-1-name".to_string(),
                        ..default()
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(report.client_id.as_str(), "ven-1-client-id");

        let (status, deleted) = bl
            .request::<Report>(
                http::Method::DELETE,
                &format!("/reports/{}", report.id),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(deleted.id, report.id);
    }

    #[sqlx::test(fixtures("programs", "events", "vens"))]
    async fn ven_client_cannot_delete_other_vens_report(db: PgPool) {
        let ven1 = ApiTest::new(db.clone(), "ven-1-client-id", vec![Scope::WriteReportsVen]).await;
        let ven2 = ApiTest::new(db, "ven-2-client-id", vec![Scope::WriteReportsVen]).await;

        let (status, report) = ven1
            .request::<Report>(
                http::Method::POST,
                "/reports",
                Body::from(
                    serde_json::to_vec(&ReportRequest {
                        event_id: "event-1".parse().unwrap(),
                        client_name: "ven-1-name".to_string(),
                        ..default()
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = ven2
            .request::<Problem>(
                http::Method::DELETE,
                &format!("/reports/{}", report.id),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(fixtures("programs", "events", "vens"))]
    async fn report_names_are_not_unique(db: PgPool) {
        let ven1 = ApiTest::new(db.clone(), "ven-1-client-id", vec![Scope::WriteReportsVen]).await;
        let ven2 = ApiTest::new(db, "ven-2-client-id", vec![Scope::WriteReportsVen]).await;

        let (status, _) = ven1
            .request::<Report>(
                http::Method::POST,
                "/reports",
                Body::from(
                    serde_json::to_vec(&ReportRequest {
                        event_id: "event-1".parse().unwrap(),
                        client_name: "ven-1-name".to_string(),
                        report_name: Some("not-unique".to_string()),
                        ..default()
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);

        let (status, _) = ven2
            .request::<Report>(
                http::Method::POST,
                "/reports",
                Body::from(
                    serde_json::to_vec(&ReportRequest {
                        event_id: "event-2".parse().unwrap(),
                        client_name: "ven-2-name".to_string(),
                        report_name: Some("not-unique".to_string()),
                        ..default()
                    })
                    .unwrap(),
                ),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    #[sqlx::test(fixtures("programs", "events", "vens", "users"))]
    async fn report_creation_via_mqtt(db: PgPool) {
        let server = ApiTest::new(db, "ven-100-client-id", vec![Scope::WriteReportsVen]).await;

        let vtn_config = server.vtn_config();
        let mut mqtt_client = paho_mqtt::AsyncClient::new(paho_mqtt::CreateOptions::new()).unwrap();
        mqtt_client
            .connect(
                paho_mqtt::ConnectOptionsBuilder::new()
                    .server_uris(&[&vtn_config.mqtt_url.as_ref().unwrap()])
                    .user_name(vtn_config.mqtt_username.as_ref().unwrap())
                    .password(vtn_config.mqtt_password.as_ref().unwrap().as_bytes())
                    .finalize(),
            )
            .await
            .unwrap();
        mqtt_client
            .subscribe(
                format!("{}push/#", vtn_config.mqtt_topic_prefix),
                QoS::ExactlyOnce,
            )
            .await
            .unwrap();
        let mqtt_rx = mqtt_client.get_stream(None);

        let expect_msg = async |object_type: ObjectType, operation: Operation, topics: &[&str]| {
            let mut topics = topics.iter().copied().collect::<BTreeSet<&str>>();
            while !topics.is_empty() {
                let msg = tokio::time::timeout(Duration::from_millis(5000), mqtt_rx.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if !topics.remove(
                    msg.topic()
                        .strip_prefix(&vtn_config.mqtt_topic_prefix)
                        .unwrap(),
                ) {
                    panic!("unexpected message {msg:?}");
                }
                let msg_data: MqttPushNotification = serde_json::from_slice(msg.payload()).unwrap();
                assert_eq!(msg_data.object_type, object_type);
                assert_eq!(msg_data.operation, operation);
            }
            if let Ok(Ok(msg)) =
                tokio::time::timeout(Duration::from_millis(50), mqtt_rx.recv()).await
            {
                panic!("stray message {msg:?}")
            }
        };

        let (code, topics) = server
            .request::<NotifierTopicsResponse>(
                http::Method::GET,
                "/notifiers/mqtt/topics/upload/report",
                Body::empty(),
            )
            .await;
        assert_eq!(code, StatusCode::OK);

        mqtt_client
            .publish(Message::new(
                topics.topics.create.unwrap(),
                serde_json::to_vec(&MqttUploadRequest {
                    object_type: ObjectType::Report,
                    operation: UploadOperation::Post,
                    object: ReportRequest {
                        event_id: "event-1".parse().unwrap(),
                        client_name: "ven-1-name".to_string(),
                        report_name: Some("not-unique".to_string()),
                        ..default()
                    },
                })
                .unwrap(),
                QoS::AtMostOnce,
            ))
            .await
            .unwrap();

        expect_msg(
            ObjectType::Report,
            Operation::Create,
            &["push/reports/create"],
        )
        .await;
    }
}
