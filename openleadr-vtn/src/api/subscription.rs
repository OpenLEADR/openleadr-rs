use std::collections::HashMap;

use axum::{
    extract,
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
    Json,
};
use openleadr_wire::{
    subscription::{Notification, NotifiersResponse},
    ClientId,
};
use tokio::sync::{mpsc, Mutex};

use crate::{error::AppError, jwt::User, state::AppState};

pub(crate) struct State {
    websockets: Mutex<HashMap<ClientId, mpsc::UnboundedSender<Notification>>>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            websockets: Mutex::new(HashMap::new()),
        }
    }
}

pub(crate) async fn notifier_get() -> Json<NotifiersResponse> {
    Json(NotifiersResponse { websocket: true })
}

pub(crate) async fn notifier_websocket_get(
    extract::State(state): extract::State<AppState>,
    User(user): User,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let client_id = user.client_id()?;

    let mut websockets = state.notifier.websockets.lock().await;
    if websockets.contains_key(&client_id) {
        return Err(AppError::Conflict(
            "websocket connection already open".to_owned(),
            None,
        ));
    }
    let (tx, mut rx) = mpsc::unbounded_channel(); // FIXME use bounded channel
    websockets.insert(client_id.clone(), tx);
    drop(websockets);

    Ok(ws.on_upgrade(|mut socket| async move {
        while let Some(msg) = rx.recv().await {
            if socket
                .send(Message::Text(serde_json::to_string(&msg).unwrap().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        state.notifier.websockets.lock().await.remove(&client_id);
    }))
}
