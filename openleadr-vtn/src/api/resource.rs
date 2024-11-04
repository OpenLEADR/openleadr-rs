use std::sync::Arc;

use aide::{
    gen::GenContext,
    openapi::{Operation, Parameter, ParameterData, ParameterSchemaOrContent, QueryStyle, ReferenceOr, SchemaObject},
    transform::TransformOperation,
    OperationInput
};
use axum::{
    extract::{Path, State},
    Json,
};
use openleadr_wire::ven::VenId;
use indexmap::map::IndexMap;
use schemars::{
    schema::Schema,
    JsonSchema
};
use serde::Deserialize;
use tracing::{info, trace};
use validator::{Validate, ValidationError};

use openleadr_wire::{
    resource::{Resource, ResourceContent, ResourceId},
    target::TargetLabel,
};

use crate::{
    api::{AppResponse, Created, StatusCodeJson, ValidatedJson, ValidatedQuery},
    data_source::ResourceCrud,
    error::AppError,
    jwt::User,
};

fn has_write_permission(User(claims): &User, ven_id: &VenId) -> Result<(), AppError> {
    if claims.is_ven_manager() {
        return Ok(());
    }

    if claims.is_ven() && claims.ven_ids().contains(ven_id) {
        return Ok(());
    }

    Err(AppError::Forbidden(
        "User not authorized to access this resource",
    ))
}

pub fn get_all_openapi(operation: TransformOperation) -> TransformOperation {
    operation
        .tag("vens")
        .summary("search ven resources")
        .id("searchVenResources")
        .description("Return the ven resources specified by venID specified in path.")
        .security_requirement_scopes("oAuth2ClientCredentials", vec!["read_all"])
        .security_requirement_scopes::<Vec<&str>, &str>("bearerAuth", vec![])
}

pub async fn get_all(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path(ven_id): Path<VenId>,
    ValidatedQuery(query_params): ValidatedQuery<QueryParams>,
    user: User,
) -> AppResponse<Vec<Resource>> {
    has_write_permission(&user, &ven_id)?;
    trace!(?query_params);

    let resources = resource_source
        .retrieve_all(ven_id, &query_params, &user)
        .await?;

    Ok(Json(resources))
}

pub async fn get(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    user: User,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let ven = resource_source.retrieve(&id, ven_id, &user).await?;

    Ok(Json(ven))
}

pub fn add_openapi(operation: TransformOperation) -> TransformOperation {
    operation
        .tag("vens")
        .summary("create resource")
        .id("createResource")
        .description("Create a new resource.")
        .security_requirement_scopes("oAuth2ClientCredentials", vec!["read_all"])
        .security_requirement_scopes::<Vec<&str>, &str>("bearerAuth", vec![])
}

pub async fn add(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    user: User,
    Path(ven_id): Path<VenId>,
    ValidatedJson(new_resource): ValidatedJson<ResourceContent>,
) -> Result<StatusCodeJson<Created, Resource>, AppError> {
    has_write_permission(&user, &ven_id)?;
    let ven = resource_source.create(new_resource, ven_id, &user).await?;

    Ok(StatusCodeJson::new(Json(ven)))
}

pub async fn edit(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    user: User,
    ValidatedJson(content): ValidatedJson<ResourceContent>,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let resource = resource_source.update(&id, ven_id, content, &user).await?;

    info!(%resource.id, resource.resource_name=resource.content.resource_name, "resource updated");

    Ok(Json(resource))
}

pub async fn delete(
    State(resource_source): State<Arc<dyn ResourceCrud>>,
    Path((ven_id, id)): Path<(VenId, ResourceId)>,
    user: User,
) -> AppResponse<Resource> {
    has_write_permission(&user, &ven_id)?;
    let resource = resource_source.delete(&id, ven_id, &user).await?;
    info!(%id, "deleted resource");
    Ok(Json(resource))
}

#[derive(Deserialize, Validate, Debug, JsonSchema)]
#[validate(schema(function = "validate_target_type_value_pair"))]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    #[validate(length(min = 1, max = 128))]
    pub(crate) resource_name: Option<String>,
    pub(crate) target_type: Option<TargetLabel>,
    pub(crate) target_values: Option<Vec<String>>,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub(crate) skip: i64,
    #[validate(range(min = 1, max = 50))]
    #[serde(default = "get_50")]
    pub(crate) limit: i64,
}

impl OperationInput for QueryParams {
    fn operation_input(ctx: &mut GenContext, operation: &mut Operation) {
        fn query_param_openapi(
            name: &str,
            description: Option<&str>,
            required: bool,
            format: ParameterSchemaOrContent,
            explode: Option<bool>,
            allow_reserved: bool
        ) -> ReferenceOr<Parameter> {
            // Some of fields we can't omit, even though it's technically valid to in openapi.
            ReferenceOr::Item(Parameter::Query {
                parameter_data: ParameterData {
                    name: name.to_string(),
                    description: description.map(|str| str.to_string()),
                    required,
                    deprecated: None,
                    format,
                    example: None,
                    examples: IndexMap::default(),
                    explode,
                    extensions: IndexMap::default()
                },
                allow_reserved,
                style: QueryStyle::default(),
                allow_empty_value: None
            })
        }
        // Extract the json schema for a given field from QueryParams.
        fn format_for_query_param(ctx: &mut GenContext, field: &str) -> ParameterSchemaOrContent {
            let schema: Schema = QueryParams::json_schema(&mut ctx.schema);
            let msg = format!("When generating openapi documentation, the field {} was not obtained from the QueryParams schema {:?}", field, schema);
            let json_schema: Schema = schema
                .into_object() // Boolean schemas converted here hit expect below.
                .object
                .and_then(|object| object
                    .properties
                    .get(field)
                    .map(|v| v.clone())
                )
                .expect(msg.as_str()); // TODO Verify this does not occur at runtime with a unit test.
            ParameterSchemaOrContent::Schema(SchemaObject {
                json_schema,
                external_docs: None,
                example: None
            })
        }
        let parameters = vec![
            query_param_openapi(
                "targetType",
                Some("Indicates targeting type, e.g. GROUP"),
                false,
                format_for_query_param(ctx, "targetType"),
                None,
                false
            ),
            query_param_openapi(
                "targetValues",
                Some("List of target values, e.g. group names"),
                false,
                format_for_query_param(ctx, "targetValues"),
                None,
                false
            ),
            query_param_openapi(
                "skip",
                Some("number of records to skip for pagination."),
                false,
                format_for_query_param(ctx, "skip"),
                Some(true),
                false
            ),
            query_param_openapi(
                "limit",
                Some("maximum number of records to return."),
                false,
                format_for_query_param(ctx, "limit"),
                Some(true),
                false
            )
        ];
        operation.parameters
            .extend(parameters.into_iter());
    }
}

fn validate_target_type_value_pair(query: &QueryParams) -> Result<(), ValidationError> {
    if query.target_type.is_some() == query.target_values.is_some() {
        Ok(())
    } else {
        Err(ValidationError::new("targetType and targetValues query parameter must either both be set or not set at the same time."))
    }
}

fn get_50() -> i64 {
    50
}

#[cfg(test)]
mod test {
    use crate::{api::test::ApiTest, jwt::AuthRole};
    use axum::body::Body;
    use openleadr_wire::{
        problem::Problem,
        resource::{Resource, ResourceContent},
    };
    use reqwest::{Method, StatusCode};
    use sqlx::PgPool;

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn test_get_all(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/vens/ven-1/resources", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/vens/ven-2/resources", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 3);

        // test with ven user
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/vens/ven-1/resources", Body::empty())
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);

        let (status, _) = test
            .request::<serde_json::Value>(Method::GET, "/vens/ven-2/resources", Body::empty())
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn get_all_filtered(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/vens/ven-1/resources?skip=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(Method::GET, "/vens/ven-1/resources?limit=1", Body::empty())
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(
                Method::GET,
                "/vens/ven-1/resources?targetType=GROUP&targetValues=group-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 1);

        let (status, resources) = test
            .request::<Vec<Resource>>(
                Method::GET,
                "/vens/ven-1/resources?targetType=GROUP&targetValues=group-1&targetValues=group-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resources.len(), 2);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn get_single_resource(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                "/vens/ven-1/resources/resource-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.id.as_str(), "resource-1");

        // test with ven user
        let test = ApiTest::new(db, vec![AuthRole::VEN("ven-1".parse().unwrap())]);

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                "/vens/ven-1/resources/resource-1",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.id.as_str(), "resource-1");

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/vens/ven-1/resources/resource-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                "/vens/ven-2/resources/resource-2",
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[sqlx::test(fixtures("users", "vens", "resources"))]
    async fn add_edit_delete(db: PgPool) {
        let test = ApiTest::new(db.clone(), vec![AuthRole::VenManager]);

        let (status, resource) = test
            .request::<Resource>(
                Method::POST,
                "/vens/ven-1/resources",
                Body::from(r#"{"resourceName":"new-resource"}"#),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resource.content.resource_name, "new-resource");

        let resource_id = resource.id.as_str();

        let (status, resource) = test
            .request::<Resource>(
                Method::PUT,
                &format!("/vens/ven-1/resources/{resource_id}"),
                Body::from(r#"{"resourceName":"updated-resource"}"#),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.content.resource_name, "updated-resource");

        let (status, resource) = test
            .request::<Resource>(
                Method::GET,
                &format!("/vens/ven-1/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resource.content.resource_name, "updated-resource");

        let (status, _) = test
            .request::<Resource>(
                Method::DELETE,
                &format!("/vens/ven-1/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) = test
            .request::<Problem>(
                Method::GET,
                &format!("/vens/ven-1/resources/{resource_id}"),
                Body::empty(),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[sqlx::test(fixtures("users", "vens"))]
    async fn name_constraint_validation(db: PgPool) {
        let test = ApiTest::new(db, vec![AuthRole::AnyBusiness]);

        let resources = [
            ResourceContent{resource_name: "".to_string(), targets: None, attributes:None},
            ResourceContent{resource_name: "This is more than 128 characters long and should be rejected This is more than 128 characters long and should be rejected asdfasd".to_string(),targets: None, attributes:None},
        ];

        for resource in &resources {
            let (status, error) = test
                .request::<Problem>(
                    Method::POST,
                    "/vens/ven-1/resources",
                    Body::from(serde_json::to_vec(&resource).unwrap()),
                )
                .await;

            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(error
                .detail
                .unwrap()
                .contains("outside of allowed range 1..=128"))
        }
    }
}
