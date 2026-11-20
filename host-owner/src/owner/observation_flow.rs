use std::task::{Context, Poll};

use super::{ordering::Wire, *};
use tokio::io::{ReadBuf, sink};

struct Stdout {
    frames: std::vec::IntoIter<Vec<u8>>,
    reads: watch::Sender<usize>,
}

impl AsyncRead for Stdout {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // One frame per read so we can witness when the transport next
        // advances, without depending on a pipe buffer size or a timer.
        if let Some(frame) = self.frames.next() {
            buf.put_slice(&frame);
            self.reads.send_modify(|count| *count += 1);
        }
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn native_stdout_waits_for_frame_and_byte_capacity_without_losing_observations() {
    for frame_limited in [true, false] {
        let first = json!({
            "type": "prompt_result",
            "id": "first-observation-awaiting-a-lifecycle-ack",
            "status": "completed",
        });
        let filler = json!({"type": "agent_start"});
        let (capacity, bytes) = if frame_limited {
            (1, 4096)
        } else {
            (
                4,
                serde_json::to_vec(&first).unwrap().len()
                    + serde_json::to_vec(&filler).unwrap().len()
                    + 2, // JSONL delimiters
            )
        };
        let (native, pump) = Channel::new(Link::Stdio);
        let (control, control_pump) = Channel::new(Link::Control);
        let (observations, queue) = mpsc::channel(capacity);
        let (progress, observed) = watch::channel(0);
        let owner = Arc::new(Owner {
            native,
            helper: Helper {
                bun: OsString::new(),
                script: PathBuf::new(),
                manifest: PathBuf::new(),
                cwd: PathBuf::new(),
                inventory: None,
            },
            control: OnceLock::from(control.clone()),
            identity: OnceLock::new(),
            native_pid: 1,
            state: Mutex::default(),
            observations,
            observation_bytes: Arc::new(Semaphore::new(bytes)),
            observed,
            inventory_slots: Semaphore::new(1),
        });
        let (lifecycle, fake_control) = UnixStream::pair().unwrap();
        let (input, output) = lifecycle.into_split();
        let _control_reader = control.start(control_pump, input, output, |_, _| ready(()), ready);
        let mut lifecycle = Wire::new(fake_control);
        let (host, client) = UnixStream::pair().unwrap();
        owner.connect(host).unwrap();
        let mut peer = Wire::new(client);
        assert_eq!(peer.frame().await["type"], "ready");
        let worker = Arc::clone(&owner);
        let _worker = tokio::spawn(async move { worker.observe(queue, progress).await });

        owner.native_event(first.clone()).await.unwrap();
        let first_observation = lifecycle.frame().await;
        assert_eq!(first_observation["frame"], first);
        owner.native_event(filler.clone()).await.unwrap();

        {
            let mut state = owner.state().unwrap();
            state
                .requests
                .insert("active".into(), "client-active".into());
            state
                .requests
                .insert("no-agent".into(), "client-no-agent".into());
        }
        let query = owner
            .native
            .begin_request(json!({"type": "get_entries"}), "query")
            .unwrap();
        let no_agent = owner
            .native
            .begin_request(json!({"type": "prompt"}), "no-agent")
            .unwrap();
        let late_error = json!({"type": "response", "command": "prompt", "id": "active",
            "success": false, "error": "prompt failed"});
        let error = json!({"type": "error", "id": "active", "message": "prompt failed"});
        let terminal = json!({"type": "prompt_result", "id": "active", "status": "failed"});
        let no_agent_response = json!({"type": "response", "command": "prompt", "id": "no-agent",
            "success": true, "data": {"agentInvoked": false}});
        let query_response = json!({"type": "response", "command": "get_entries", "id": "query",
            "success": true, "data": {"entries": []}});
        let mut frames = vec![error.clone(), late_error, terminal.clone()];
        frames.insert(if frame_limited { 3 } else { 0 }, no_agent_response.clone());
        frames.push(query_response.clone());
        let (reads, mut read_count) = watch::channel(0);
        let stdout = Stdout {
            frames: frames
                .into_iter()
                .map(|frame| {
                    let mut line = serde_json::to_vec(&frame).unwrap();
                    line.push(b'\n');
                    line
                })
                .collect::<Vec<_>>()
                .into_iter(),
            reads,
        };

        let drain = owner.start_native(pump, stdout, sink());
        assert_eq!(
            *timeout(REQUEST_TIMEOUT, read_count.wait_for(|count| *count != 0))
                .await
                .unwrap()
                .unwrap(),
            1,
            "native stdout advanced beyond the first frame without capacity",
        );
        assert!(
            !owner
                .state
                .try_lock()
                .expect("backpressure held residency lock")
                .fenced(),
            "transient backpressure fenced residency",
        );
        lifecycle
            .reply(
                &first_observation,
                json!({"success": true, "data": {"entryId": "first"}}),
            )
            .await;
        let durable_ids = if frame_limited {
            ["client-active", "client-no-agent"]
        } else {
            ["client-no-agent", "client-active"]
        };
        for id in durable_ids {
            let observation = lifecycle.frame().await;
            assert_eq!(observation["type"], "observe");
            assert_eq!(observation["frame"]["id"], id);
            assert_eq!(observation["runOrdinal"], 1);
            lifecycle
                .reply(
                    &observation,
                    json!({"success": true, "data": {"entryId": id}}),
                )
                .await;
        }
        assert_eq!(
            no_agent.response(Some(REQUEST_TIMEOUT)).await.unwrap(),
            no_agent_response
        );
        assert_eq!(
            query.response(Some(REQUEST_TIMEOUT)).await.unwrap(),
            query_response
        );
        assert!(timeout(REQUEST_TIMEOUT, drain).await.unwrap().unwrap());
        timeout(REQUEST_TIMEOUT, owner.observations_flushed())
            .await
            .unwrap()
            .unwrap();
        owner.prompt_acknowledged("no-agent").await.unwrap();

        let mut error = error;
        error["id"] = json!("client-active");
        let mut terminal = terminal;
        terminal["id"] = json!("client-active");
        for event in [first, filler, error, terminal] {
            assert_eq!(peer.frame().await, event);
        }
        assert!(!owner.state().unwrap().fenced());
    }
}
