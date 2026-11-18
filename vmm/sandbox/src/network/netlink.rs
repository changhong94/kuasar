/*
Copyright 2022 The Kuasar Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use futures_util::StreamExt;
use netlink_packet_core::{NetlinkMessage, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REQUEST};
use netlink_packet_route::{
    tc::{
        TcAction, TcActionAttribute, TcActionGeneric, TcActionMirrorOption, TcActionOption,
        TcActionType, TcAttribute, TcFilterMatchAllOption, TcMessage, TcMirror, TcMirrorActionType,
        TcOption,
    },
    RouteNetlinkMessage,
};
use netlink_packet_utils::{nla::DefaultNla, Emitable};
use rtnetlink::{try_nl, Error, Handle};

const HANDLE_INGRESS: u32 = 0xfffffff1;
const HANDLE_TC_FILTER: u32 = 0xffff0000;

pub struct QDiscAddRequest {
    handle: Handle,
    message: TcMessage,
}

#[allow(dead_code)]
impl QDiscAddRequest {
    pub(crate) fn new(handle: Handle) -> Self {
        QDiscAddRequest {
            handle,
            message: TcMessage::default(),
        }
    }

    pub async fn execute(self) -> Result<(), Error> {
        let QDiscAddRequest {
            mut handle,
            message,
        } = self;

        let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewQueueDiscipline(message));
        req.header.flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK;

        let mut response = handle.request(req)?;
        while let Some(message) = response.next().await {
            try_nl!(message);
        }
        Ok(())
    }

    pub fn if_index(mut self, if_index: i32) -> Self {
        self.message.header.index = if_index;
        self
    }

    pub fn ingress(mut self) -> Self {
        self.message.header.parent = HANDLE_INGRESS.into();
        self
    }
}

pub struct TrafficFilterSetRequest {
    handle: Handle,
    message: TcMessage,
}

#[allow(dead_code)]
impl TrafficFilterSetRequest {
    pub(crate) fn new(handle: Handle, ifindex: i32) -> Self {
        let mut message = TcMessage::default();
        message.header.index = ifindex;
        message.header.parent = HANDLE_TC_FILTER.into();

        Self { handle, message }
    }

    pub async fn execute(self) -> Result<(), Error> {
        let TrafficFilterSetRequest {
            mut handle,
            message,
        } = self;

        let mut req = NetlinkMessage::from(RouteNetlinkMessage::NewTrafficFilter(message));
        req.header.flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK;

        let mut response = handle.request(req)?;
        while let Some(message) = response.next().await {
            try_nl!(message);
        }
        Ok(())
    }

    pub fn mirror(self, _dest_index: i32) -> Self {
        //TODO
        self
    }
}

/// Install a qdisc or filter and wait for the kernel ACK before publication.
async fn add_tc(mut handle: Handle, message: RouteNetlinkMessage) -> Result<(), Error> {
    let mut req = NetlinkMessage::from(message);
    req.header.flags = NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK;
    let mut response = handle.request(req)?;
    while let Some(message) = response.next().await {
        try_nl!(message);
    }
    Ok(())
}

pub(crate) async fn add_ingress(handle: Handle, index: u32) -> Result<(), Error> {
    let mut message = TcMessage::default();
    message.header.index = index as i32;
    message.header.handle = HANDLE_TC_FILTER.into();
    message.header.parent = HANDLE_INGRESS.into();
    message.attributes = vec![TcAttribute::Kind("ingress".into())];
    add_tc(handle, RouteNetlinkMessage::NewQueueDiscipline(message)).await
}

async fn add_matchall(
    handle: Handle,
    index: u32,
    priority: u16,
    action: TcAction,
) -> Result<(), Error> {
    let mut message = TcMessage::default();
    message.header.index = index as i32;
    message.header.parent = 0xfffffff2u32.into();
    // tcmsg.tcm_info: priority in the high half, ETH_P_ALL in network order.
    message.header.info = (u32::from(priority) << 16) | u32::from(3u16.to_be());
    message.attributes = vec![
        TcAttribute::Kind("matchall".into()),
        TcAttribute::Options(vec![TcOption::MatchAll(TcFilterMatchAllOption::Action(
            vec![action],
        ))]),
    ];
    add_tc(handle, RouteNetlinkMessage::NewTrafficFilter(message)).await
}

pub(crate) async fn add_redirect(
    handle: Handle,
    index: u32,
    destination: u32,
) -> Result<(), Error> {
    let mut mirror = TcMirror::default();
    mirror.generic.action = TcActionType::Stolen;
    mirror.eaction = TcMirrorActionType::EgressRedir;
    mirror.ifindex = destination;
    let mut action = TcAction::default();
    action.attributes = vec![
        TcActionAttribute::Kind("mirred".into()),
        TcActionAttribute::Options(vec![TcActionOption::Mirror(TcActionMirrorOption::Parms(
            mirror,
        ))]),
    ];
    // Keep priority 1 for the reset barrier and 2.. for stale-parent rejection.
    add_matchall(handle, index, 100, action).await
}

pub(crate) async fn add_fork_drop_filter(handle: Handle, index: u32) -> Result<(), Error> {
    let mut generic = TcActionGeneric::default();
    generic.action = TcActionType::Shot;
    let mut parameters = vec![0; generic.buffer_len()];
    generic.emit(&mut parameters);
    let mut action = TcAction::default();
    // This library version has no typed gact option. Linux tc_gact consists
    // exactly of tc_gen; TCA_GACT_PARMS is 2 (linux/tc_act/tc_gact.h).
    action.attributes = vec![
        TcActionAttribute::Kind("gact".into()),
        TcActionAttribute::Options(vec![TcActionOption::Other(DefaultNla::new(2, parameters))]),
    ];
    add_matchall(handle, index, 1, action).await
}

/// Equivalent to `tc filter del dev TAP ingress pref 1`, including kernel ACK.
/// Leave the mirroring filters at other priorities and the qdisc intact.
pub(crate) async fn delete_fork_drop_filter(mut handle: Handle, index: u32) -> Result<(), Error> {
    let mut message = TcMessage::default();
    message.header.index = index as i32;
    message.header.parent = 0xfffffff2u32.into(); // TC_H_MIN_INGRESS
    message.header.info = 1 << 16; // priority 1, wildcard protocol
    let mut req = NetlinkMessage::from(RouteNetlinkMessage::DelTrafficFilter(message));
    req.header.flags = NLM_F_REQUEST | NLM_F_ACK;
    let mut response = handle.request(req)?;
    while let Some(message) = response.next().await {
        try_nl!(message);
    }
    Ok(())
}

#[cfg(test)]
mod fork_filter_tests {
    use futures_util::TryStreamExt;

    use super::*;
    #[tokio::test]
    #[ignore = "requires root in a fresh private network namespace"]
    async fn delete_only_fork_priority() {
        assert_ne!(
            std::fs::read_link("/proc/self/ns/net").unwrap(),
            std::fs::read_link("/proc/1/ns/net").unwrap()
        );
        for args in [
            vec!["ip", "link", "add", "forkdiag0", "type", "dummy"],
            vec!["ip", "link", "add", "forkdiag1", "type", "dummy"],
        ] {
            assert!(std::process::Command::new(args[0])
                .args(&args[1..])
                .status()
                .unwrap()
                .success());
        }
        let (connection, handle, _) = rtnetlink::new_connection().unwrap();
        tokio::spawn(connection);
        let link = handle
            .link()
            .get()
            .match_name("forkdiag0".into())
            .execute()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        let peer = handle
            .link()
            .get()
            .match_name("forkdiag1".into())
            .execute()
            .try_next()
            .await
            .unwrap()
            .unwrap();
        add_ingress(handle.clone(), link.header.index)
            .await
            .unwrap();
        add_redirect(handle.clone(), link.header.index, peer.header.index)
            .await
            .unwrap();
        add_fork_drop_filter(handle.clone(), link.header.index)
            .await
            .unwrap();
        assert!(add_fork_drop_filter(handle.clone(), link.header.index)
            .await
            .is_err());
        let out = std::process::Command::new("tc")
            .args(["-j", "filter", "show", "dev", "forkdiag0", "ingress"])
            .output()
            .unwrap();
        let filters: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        let drop = filters
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["pref"] == 1 && !f["options"].is_null())
            .unwrap();
        assert_eq!(
            drop["options"]["actions"][0]["control_action"]["type"],
            "drop"
        );
        delete_fork_drop_filter(handle.clone(), link.header.index)
            .await
            .unwrap();
        let out = std::process::Command::new("tc")
            .args(["-j", "filter", "show", "dev", "forkdiag0", "ingress"])
            .output()
            .unwrap();
        assert!(out.status.success());
        let filters: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        assert!(!filters.as_array().unwrap().is_empty());
        assert!(filters.as_array().unwrap().iter().all(|f| f["pref"] == 100));
        assert!(delete_fork_drop_filter(handle, link.header.index)
            .await
            .is_err());
    }
}
