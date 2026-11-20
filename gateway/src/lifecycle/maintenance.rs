use anyhow::Result;
use serde_json::{Value, json};
use tokio::{task::JoinHandle, time::Instant};

use super::Lifecycle;

#[derive(Default)]
pub(super) struct Maintenance {
    next: Option<Instant>,
    task: Option<JoinHandle<()>>,
    pub status: Option<Value>,
}

impl Lifecycle {
    pub(super) fn schedule_maintenance(&self) -> Result<()> {
        let Some(ulw) = self.0.ulw.get().filter(|ulw| ulw.can_update()) else {
            return Ok(());
        };
        let mut maintenance = self
            .0
            .maintenance
            .lock()
            .map_err(|_| anyhow::anyhow!("ULW maintenance is unavailable"))?;
        let now = Instant::now();
        if *self.0.closing.borrow()
            || maintenance.next.is_some_and(|next| now < next)
            || maintenance
                .task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
        {
            return Ok(());
        }
        maintenance.next = Some(now + ulw.interval);
        if let Some(status) = maintenance.status.as_mut() {
            status["updating"] = json!(true);
        }
        let owner = self.clone();
        maintenance.task = Some(tokio::spawn(async move {
            let Some(ulw) = owner.0.ulw.get() else {
                return;
            };
            let update = ulw.update(&owner.0.config).await;
            let result = ulw.status();
            if update.is_err() || result.is_err() {
                eprintln!("Managed ULW update or status failed; inspect private package state");
            }
            match owner.0.maintenance.lock() {
                Ok(mut maintenance) => {
                    if let Ok(status) = result {
                        maintenance.status = Some(status);
                    }
                    if let Some(status) = maintenance.status.as_mut() {
                        status["updating"] = json!(false);
                        if update.is_err() && status["error"].is_null() {
                            status["error"] = json!("Managed ULW update failed");
                        }
                    }
                }
                Err(_) => eprintln!("ULW maintenance state is unavailable"),
            }
        }));
        Ok(())
    }

    pub async fn wait_maintenance(&self) {
        let task = match self.0.maintenance.lock() {
            Ok(mut maintenance) => maintenance.task.take(),
            Err(_) => {
                eprintln!("ULW maintenance state is unavailable");
                return;
            }
        };
        if let Some(task) = task
            && task.await.is_err()
        {
            eprintln!("ULW maintenance task did not finish normally");
        }
    }
}
