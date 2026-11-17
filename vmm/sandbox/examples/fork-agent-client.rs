// SPDX-License-Identifier: Apache-2.0
//! Integration driver: real Kuasar Controller API and guest Task service.
use std::{collections::HashMap, os::fd::IntoRawFd};

use anyhow::{bail, Result};
use containerd_sandbox::{
    api::sandbox::v1::{
        controller_client::ControllerClient, ControllerStatusRequest, ControllerUpdateRequest,
    },
    types::Sandbox,
};
use containerd_shim::protos::{
    api::{
        CreateTaskRequest, DeleteRequest, ExecProcessRequest, KillRequest, StartRequest,
        StateRequest, WaitRequest,
    },
    shim::shim_ttrpc_async::TaskClient,
};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use ttrpc::{context::with_timeout, r#async::Client};
use vmm_common::api::{
    sandbox::{CheckRequest, ExecVMProcessRequest},
    sandbox_ttrpc::SandboxServiceClient,
};

async fn connect(path: &str) -> Result<Client> {
    let mut s = tokio::net::UnixStream::connect(path).await?;
    s.write_all(b"CONNECT 1024\n").await?;
    let mut line = Vec::new();
    loop {
        let b = s.read_u8().await?;
        line.push(b);
        if b == b'\n' {
            break;
        }
    }
    if !line.starts_with(b"OK ") {
        bail!("vsock handshake: {:?}", line);
    }
    Ok(Client::new(s.into_std()?.into_raw_fd()))
}
#[tokio::main]
async fn main() -> Result<()> {
    let a: Vec<String> = std::env::args().collect();
    let op = a[1].as_str();
    let socket = &a[2];
    let client = connect(socket).await?;
    let agent = SandboxServiceClient::new(client.clone());
    let task = TaskClient::new(client);
    let ctx = || with_timeout(15_000_000_000);
    match op {
        "exec" => {
            let r = agent
                .exec_vm_process(
                    ctx(),
                    &ExecVMProcessRequest {
                        command: a[3].clone(),
                        ..Default::default()
                    },
                )
                .await?;
            print!("{}", r.out);
        }
        "hold" => {
            agent.check(ctx(), &CheckRequest::new()).await?;
            println!("CONNECTED");
            std::io::Write::flush(&mut std::io::stdout())?;
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            if agent
                .check(with_timeout(2_000_000_000), &CheckRequest::new())
                .await
                .is_ok()
            {
                bail!("pre-fork RPC connection survived reset");
            }
            println!("OLD_CONNECTION_RESET");
        }
        "create" => {
            let id = &a[3];
            let sid = &a[4];
            let grpc = a[5].clone();
            let value = &a[6];
            let mut spec = json!({"ociVersion":"1.0.2","process":{"terminal":false,"user":{"uid":0,"gid":0},
                "args":["/bin/sh","-c",format!("echo {} > /value; while :; do echo {} >> /heartbeat; /bin/busybox sleep 1; done",value,value)],
                "env":["PATH=/bin"],"cwd":"/"}, "root":{"path":"rootfs","readonly":false},
                "linux":{"namespaces":[{"type":"mount"},{"type":"pid"}],"resources":{}},
                "mounts":[{"destination":"/proc","type":"proc","source":"proc"},
                          {"destination":"/dev","type":"tmpfs","source":"tmpfs"}]});
            if a.get(7).is_some_and(|mode| mode == "inherited") {
                spec["process"]["args"] = json!(["/bin/fork-workload", "init"]);
            }
            let tasks = json!({"tasks":[{"task_id":id,"spec":spec}]});
            let channel = tonic::transport::Endpoint::from_static("http://localhost")
                .connect_with_connector(tower::service_fn(move |_| {
                    tokio::net::UnixStream::connect(grpc.clone())
                }))
                .await?;
            let mut controller = ControllerClient::new(channel);
            let advertised = controller
                .status(ControllerStatusRequest {
                    sandbox_id: sid.clone(),
                    ..Default::default()
                })
                .await?
                .into_inner();
            assert_eq!(
                advertised.address,
                format!("ttrpc+hvsock://{}:1024", socket)
            );
            controller
                .update(ControllerUpdateRequest {
                    sandbox_id: sid.clone(),
                    sandboxer: "kuasar-vmm-sandboxer-clh".into(),
                    sandbox: Some(Sandbox {
                        extensions: HashMap::from([(
                            "tasks".into(),
                            prost_types::Any {
                                type_url: "tasks".into(),
                                value: serde_json::to_vec(&tasks)?,
                            },
                        )]),
                        ..Default::default()
                    }),
                    fields: vec!["extensions.tasks".into()],
                })
                .await?;
            let bundle = format!("/run/kuasar/state/{}", id);
            agent
                .exec_vm_process(
                    ctx(),
                    &ExecVMProcessRequest {
                        command: format!("cp -a /opt/container-rootfs {}/rootfs", bundle),
                        ..Default::default()
                    },
                )
                .await?;
            let result = task
                .create(
                    ctx(),
                    &CreateTaskRequest {
                        id: id.clone(),
                        bundle,
                        ..Default::default()
                    },
                )
                .await?;
            task.start(
                ctx(),
                &StartRequest {
                    id: id.clone(),
                    ..Default::default()
                },
            )
            .await?;
            println!("CONTAINER_RUNNING pid={}", result.pid);
        }
        "exec-create" => {
            let id = &a[3];
            let sid = &a[4];
            let grpc = a[5].clone();
            let exec_id = &a[6];
            let saved: serde_json::Value = serde_json::from_slice(&std::fs::read(
                std::path::Path::new(&grpc)
                    .parent()
                    .unwrap()
                    .join("state")
                    .join(sid)
                    .join("sandbox.json"),
            )?)?;
            let data: containerd_sandbox::data::SandboxData =
                serde_json::from_value(saved["data"].clone())?;
            let bytes = &data.extensions["tasks"].value;
            let mut tasks: serde_json::Value = serde_json::from_slice(&bytes)?;
            let spec = json!({"terminal":false,"user":{"uid":0,"gid":0},"args":["/bin/fork-workload",exec_id],"env":["PATH=/bin"],"cwd":"/"});
            tasks["tasks"][0]["processes"] = json!([{"exec_id":exec_id,"spec":spec}]);
            let channel = tonic::transport::Endpoint::from_static("http://localhost")
                .connect_with_connector(tower::service_fn(move |_| {
                    tokio::net::UnixStream::connect(grpc.clone())
                }))
                .await?;
            ControllerClient::new(channel)
                .update(ControllerUpdateRequest {
                    sandbox_id: sid.clone(),
                    sandbox: Some(Sandbox {
                        extensions: HashMap::from([(
                            "tasks".into(),
                            prost_types::Any {
                                type_url: "tasks".into(),
                                value: serde_json::to_vec(&tasks)?,
                            },
                        )]),
                        ..Default::default()
                    }),
                    fields: vec!["extensions.tasks".into()],
                    ..Default::default()
                })
                .await?;
            let mut request = ExecProcessRequest {
                id: id.clone(),
                exec_id: exec_id.clone(),
                ..Default::default()
            };
            request.spec = protobuf::MessageField::some(protobuf::well_known_types::any::Any {
                type_url: "types.containerd.io/opencontainers/runtime-spec/1/Process".into(),
                value: serde_json::to_vec(&spec)?,
                ..Default::default()
            });
            task.exec(ctx(), &request).await?;
            let r = task
                .start(
                    ctx(),
                    &StartRequest {
                        id: id.clone(),
                        exec_id: exec_id.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            println!("EXEC_RUNNING pid={}", r.pid);
        }
        "forget" => {
            let sid = &a[3];
            let grpc = a[4].clone();
            let channel = tonic::transport::Endpoint::from_static("http://localhost")
                .connect_with_connector(tower::service_fn(move |_| {
                    tokio::net::UnixStream::connect(grpc.clone())
                }))
                .await?;
            ControllerClient::new(channel)
                .update(ControllerUpdateRequest {
                    sandbox_id: sid.clone(),
                    sandbox: Some(Sandbox {
                        extensions: HashMap::from([(
                            "tasks".into(),
                            prost_types::Any {
                                type_url: "tasks".into(),
                                value: serde_json::to_vec(&json!({"tasks":[]}))?,
                            },
                        )]),
                        ..Default::default()
                    }),
                    fields: vec!["extensions.tasks".into()],
                    ..Default::default()
                })
                .await?;
            println!("METADATA_REMOVED");
        }
        "wait" => {
            println!("WAITING");
            std::io::Write::flush(&mut std::io::stdout())?;
            let result = task
                .wait(
                    with_timeout(0),
                    &WaitRequest {
                        id: a[3].clone(),
                        ..Default::default()
                    },
                )
                .await;
            println!("WAIT_RESULT {:?}", result);
        }
        "freeze" | "thaw" => {
            let r = agent
                .fork_control(
                    ctx(),
                    &vmm_common::api::sandbox::ForkControlRequest {
                        token: a[3].clone(),
                        release: op == "thaw",
                        ..Default::default()
                    },
                )
                .await?;
            println!("TASKS {}", r.containers.len());
        }
        "state" => {
            let r = task
                .state(
                    ctx(),
                    &StateRequest {
                        id: a[3].clone(),
                        exec_id: a.get(4).cloned().unwrap_or_default(),
                        ..Default::default()
                    },
                )
                .await?;
            println!("{:?} pid={}", r.status, r.pid);
        }
        "delete" => {
            task.kill(
                ctx(),
                &KillRequest {
                    id: a[3].clone(),
                    exec_id: a.get(4).cloned().unwrap_or_default(),
                    signal: 9,
                    all: a.get(4).is_none(),
                    ..Default::default()
                },
            )
            .await?;
            println!("CONTAINER_KILLED");
            task.wait(
                ctx(),
                &WaitRequest {
                    id: a[3].clone(),
                    exec_id: a.get(4).cloned().unwrap_or_default(),
                    ..Default::default()
                },
            )
            .await?;
            println!("CONTAINER_WAITED");
            let r = task
                .delete(
                    ctx(),
                    &DeleteRequest {
                        id: a[3].clone(),
                        exec_id: a.get(4).cloned().unwrap_or_default(),
                        ..Default::default()
                    },
                )
                .await?;
            println!("CONTAINER_DELETED exit={}", r.exit_status);
        }
        _ => bail!("unknown operation"),
    }
    Ok(())
}
