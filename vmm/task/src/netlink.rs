// Copyright (c) 2021 Kata Maintainers
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    convert::TryFrom,
    collections::HashMap,
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    ops::Deref,
    str::FromStr,
};

use containerd_shim::{
    error::{Error, Result},
    other, other_error,
    protos::protobuf::EnumOrUnknown,
};
use futures::{future, TryStreamExt};
use ipnetwork::{IpNetwork, Ipv4Network, Ipv6Network};
use netlink_packet_route::{
    address::{AddressAttribute, AddressMessage},
    link::{LinkAttribute, LinkFlag, LinkMessage},
    route::{
        RouteAddress, RouteAttribute, RouteHeader, RouteMessage, RouteProtocol, RouteScope,
        RouteType,
    },
    AddressFamily,
};
use nix::errno::Errno;
use rtnetlink::{new_connection, IpVersion};
use vmm_common::api::sandbox::{IPAddress, IPFamily, Interface, Route};

/// Search criteria to use when looking for a link in `find_link`.
pub enum LinkFilter<'a> {
    /// Find by link name.
    Name(&'a str),
    /// Find by link index.
    Index(u32),
    /// Find by MAC address.
    Address(&'a str),
}

impl fmt::Display for LinkFilter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LinkFilter::Name(name) => write!(f, "Name: {}", name),
            LinkFilter::Index(idx) => write!(f, "Index: {}", idx),
            LinkFilter::Address(addr) => write!(f, "Address: {}", addr),
        }
    }
}

/// A filter to query addresses.
pub enum AddressFilter {
    /// Return addresses that belong to the given interface.
    LinkIndex(u32),
}

/// A high level wrapper for netlink (and `rtnetlink` crate) for use by the Agent's RPC.
/// It is expected to be consumed by the `AgentService`, so it operates with protobuf
/// structures directly for convenience.
pub struct Handle {
    handle: rtnetlink::Handle,
}

impl Handle {
    pub(crate) fn new() -> Result<Handle> {
        let (conn, handle, _) =
            new_connection().map_err(other_error!(e, "failed to new netlink connection"))?;
        tokio::spawn(conn);

        Ok(Handle { handle })
    }

    pub(crate) async fn enable_lo(&self) -> Result<()> {
        let link = self.find_link(LinkFilter::Name("lo")).await?;
        self.enable_link(link.index(), true).await?;
        Ok(())
    }

    pub async fn reset_fork_interface(
        &mut self,
        old_mac: &str,
        intfs: Vec<Interface>,
    ) -> Result<()> {
        let link = self.find_fork_interface(old_mac, &intfs, false).await?;
        self.apply_fork_interface(intfs, link).await
    }

    // Read-only resolution may overlap entropy/TCP cleanup while the fork gate
    // and host network gate remain closed. Mutation still follows cleanup.
    pub(crate) async fn find_fork_interface(
        &self, old_mac: &str, intfs: &[Interface], certified_template: bool,
    ) -> Result<Link> {
        if intfs.len() != 1 {
            return Err(other!("fork requires one guest NIC"));
        }
        // Prefer a directed name query, but verify identity. Renamed links and
        // lost-response retries retain the old/new-MAC fallback.
        let old = parse_mac_address(old_mac)?;
        let new = parse_mac_address(&intfs[0].hwAddr)?;
        // Frozen fixed templates cannot rename/reconfigure the NIC. Read the
        // small identity tuple synchronously instead of an async netlink round
        // trip; retain full netlink lookup and MAC fallback for every failure.
        let named = if certified_template {
            match fork_link_by_name(&intfs[0].name) {
                Ok(link) => Ok(link),
                Err(_) => self.find_link(LinkFilter::Name(&intfs[0].name)).await,
            }
        } else {
            self.find_link(LinkFilter::Name(&intfs[0].name)).await
        };
        let link = match named {
            Ok(link) if link.attributes.iter().any(|a| matches!(a,
                LinkAttribute::Address(mac) if mac.as_slice() == old || mac.as_slice() == new)) => link,
            _ => match self.find_link(LinkFilter::Address(old_mac)).await {
                Ok(link) => link,
                Err(_) => self.find_link(LinkFilter::Address(&intfs[0].hwAddr)).await?,
            },
        };
        Ok(link)
    }

    pub(crate) async fn apply_fork_interface(
        &mut self, intfs: Vec<Interface>, link: Link,
    ) -> Result<()> {
        if intfs.len() != 1 { return Err(other!("fork requires one guest NIC")); }
        self.update_interface(intfs.into_iter().next().unwrap(), link, true).await
    }

    pub(crate) async fn apply_fork_routes(
        &mut self, routes: Vec<Route>, interface_name: String, interface_index: u32,
    ) -> Result<()> {
        let old_routes = self.query_routes(None).await?;
        self.delete_routes(old_routes).await?;
        let mut indices = HashMap::new();
        indices.insert(interface_name, interface_index);
        self.add_routes_with_indices(routes, indices).await
    }

    pub async fn update_interfaces(&mut self, intfs: Vec<Interface>) -> Result<()> {
        for intf in intfs {
            let link = self.find_link(LinkFilter::Address(&intf.hwAddr)).await?;
            self.update_interface(intf, link, false).await?;
        }
        Ok(())
    }

    async fn update_interface(&mut self, intf: Interface, link: Link, replace_mac: bool) -> Result<()> {
            // Bring down interface if it is UP
            if link.is_up() {
                self.enable_link(link.index(), false).await?;
            }

            // Delete all addresses associated with the link
            let addresses = self
                .list_addresses(AddressFilter::LinkIndex(link.index()))
                .await?;
            self.delete_addresses(addresses).await?;

            // Add new ip addresses from request
            for ip_address in &intf.IPAddresses {
                let ip = IpAddr::from_str(&ip_address.address).map_err(other_error!(
                    e,
                    format!("invalid ip address: {}", ip_address.address)
                ))?;
                let mask = ip_address.mask.parse::<u8>()?;

                self.add_addresses(
                    link.index(),
                    std::iter::once(
                        IpNetwork::new(ip, mask).map_err(other_error!(e, "invalid ip and mask"))?,
                    ),
                )
                .await?;
            }

            // Update link
            let mut request = self.handle.link().set(link.index());
            // Use the set-request header: copying the pre-down flags and
            // then calling up() duplicates flag bits in this crate version.

            // if there are more than one interfaces in the pod, when the kernel boot up,
            // it will give the name of the interfaces "ethX",
            // where X is the order the driver register its device to network.
            // so there are opportunities that the interface "eth1" (or "veth1" or anything else)
            // on the host become "eth0" in the guest as its original status.
            // in that case we have to change its name to a temporary name, to avoid name conflict.
            // the temporary name will be changed to the name on the host after updating that interface.
            if link.name() != intf.name.as_str() {
                let existed_link = self.find_link(LinkFilter::Name(&intf.name)).await;
                if let Ok(l) = existed_link {
                    let mut request = self.handle.link().set(l.index());
                    request.message_mut().header = l.header.clone();
                    request
                        .name(format!("{}-tmp", l.name()))
                        .execute()
                        .await
                        .map_err(other_error!(e, "failed to execute netlink request"))?;
                }
            }
            if replace_mac {
                request = request.address(parse_mac_address(&intf.hwAddr)?.to_vec());
            }
            request
                .mtu(intf.mtu as _)
                .name(intf.name.clone())
                .arp(intf.raw_flags & libc::IFF_NOARP as u32 == 0)
                .up()
                .execute()
                .await
                .map_err(other_error!(e, "failed to execute netlink request"))?;
        Ok(())
    }

    async fn find_link(&self, filter: LinkFilter<'_>) -> Result<Link> {
        let request = self.handle.link().get();

        let filtered = match filter {
            LinkFilter::Name(name) => request.match_name(name.to_owned()),
            LinkFilter::Index(index) => request.match_index(index),
            _ => request, // Post filters
        };

        let mut stream = filtered.execute();

        let next = if let LinkFilter::Address(addr) = filter {
            let mac_addr = parse_mac_address(addr)?;

            // Hardware filter might not be supported by netlink,
            // we may have to dump link list and then find the target link.
            stream
                .try_filter(|f| {
                    let result = f.attributes.iter().any(|n| match n {
                        LinkAttribute::Address(data) => data.eq(&mac_addr),
                        _ => false,
                    });

                    future::ready(result)
                })
                .try_next()
                .await
                .map_err(other_error!(e, ""))?
        } else {
            stream.try_next().await.map_err(other_error!(e, ""))?
        };

        next.map(|msg| msg.into())
            .ok_or_else(|| other!("Link not found ({})", filter))
    }

    pub async fn enable_link(&self, link_index: u32, up: bool) -> Result<()> {
        let link_req = self.handle.link().set(link_index);
        let set_req = if up { link_req.up() } else { link_req.down() };
        set_req
            .execute()
            .await
            .map_err(other_error!(e, "failed to enable link"))?;
        Ok(())
    }

    async fn list_addresses<F>(&self, filter: F) -> Result<Vec<Address>>
    where
        F: Into<Option<AddressFilter>>,
    {
        let mut request = self.handle.address().get();

        if let Some(filter) = filter.into() {
            request = match filter {
                AddressFilter::LinkIndex(index) => request.set_link_index_filter(index),
            };
        };

        let list = request
            .execute()
            .try_filter_map(|msg| future::ready(Ok(Some(Address(msg))))) // Map message to `Address`
            .try_collect()
            .await
            .map_err(other_error!(e, "failed to execute list address"))?;
        Ok(list)
    }

    async fn add_addresses<I>(&mut self, index: u32, list: I) -> Result<()>
    where
        I: IntoIterator<Item = IpNetwork>,
    {
        for net in list.into_iter() {
            self.handle
                .address()
                .add(index, net.ip(), net.prefix())
                .execute()
                .await
                .map_err(other_error!(
                    e,
                    format!("Failed to add address {}", net.ip())
                ))?;
        }

        Ok(())
    }

    async fn delete_addresses<I>(&mut self, list: I) -> Result<()>
    where
        I: IntoIterator<Item = Address>,
    {
        for addr in list.into_iter() {
            self.handle
                .address()
                .del(addr.0)
                .execute()
                .await
                .map_err(other_error!(e, "failed to del address"))?;
        }

        Ok(())
    }

    pub async fn update_routes<I>(&mut self, list: I) -> Result<()>
    where
        I: IntoIterator<Item = Route>,
    {
        let old_routes = self.query_routes(None).await?;

        self.delete_routes(old_routes).await?;

        self.add_routes(list).await?;

        Ok(())
    }

    async fn query_routes(&self, ip_version: Option<IpVersion>) -> Result<Vec<RouteMessage>> {
        let list = if let Some(ip_version) = ip_version {
            self.handle
                .route()
                .get(ip_version)
                .execute()
                .try_collect()
                .await
                .map_err(other_error!(e, "failed to query routes"))?
        } else {
            // A single AF_UNSPEC dump covers both families and avoids two
            // serialized dump transactions on this routing socket.
            let mut request = self.handle.route().get(IpVersion::V4);
            request.message_mut().header.address_family = AddressFamily::Unspec;
            request.execute().try_collect::<Vec<_>>().await
                .map_err(other_error!(e, "failed to query all routes"))?

        };

        Ok(list)
    }

    /// Adds a list of routes from iterable object `I`.
    /// It can accept both a collection of routes or a single item (via `iter::once()`).
    /// It'll also take care of proper order when adding routes (gateways first, everything else after).
    async fn add_routes<I>(&mut self, list: I) -> Result<()>
    where
        I: IntoIterator<Item = Route>,
    {
        self.add_routes_with_indices(list, HashMap::new()).await
    }

    async fn add_routes_with_indices<I>(&mut self, list: I, mut indices: HashMap<String, u32>) -> Result<()>
    where I: IntoIterator<Item = Route>,
    {
        // Split the list so we add routes with no gateway first.
        // Note: `partition_in_place` is a better fit here, since it reorders things inplace (instead of
        // allocating two separate collections), however it's not yet in stable Rust.
        let (a, b): (Vec<Route>, Vec<Route>) = list.into_iter().partition(|p| p.gateway.is_empty());
        let list = a.iter().chain(&b);

        for route in list {
            let index = if let Some(index) = indices.get(&route.device) {
                *index
            } else {
                let index = self.find_link(LinkFilter::Name(&route.device)).await?.index();
                indices.insert(route.device.clone(), index);
                index
            };

            // Build a common indeterminate ip request
            let mut request = self
                .handle
                .route()
                .add()
                .table_id(RouteHeader::RT_TABLE_MAIN as u32)
                .kind(RouteType::Unicast)
                .protocol(RouteProtocol::Boot)
                .scope(RouteScope::from(route.scope as u8));
            // Override the existing flags, as the flags sent by the client should take precedence.
            request.message_mut().header.flags = VecRouteFlag::from(route.flags).0;

            // `rtnetlink` offers a separate request builders for different IP versions (IP v4 and v6).
            // This if branch is a bit clumsy because it does almost the same.
            if route.family.enum_value_or_default() == IPFamily::v6 {
                let dest_addr = if !route.dest.is_empty() {
                    Ipv6Network::from_str(&route.dest)
                        .map_err(other_error!(e, "failed parse ipv6 network"))?
                } else {
                    Ipv6Network::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0), 0)
                        .map_err(other_error!(e, "failed new ipv6 network"))?
                };

                // Build IP v6 request
                let mut request = request
                    .v6()
                    .destination_prefix(dest_addr.ip(), dest_addr.prefix())
                    .output_interface(index);

                if !route.source.is_empty() {
                    let network = Ipv6Network::from_str(&route.source)
                        .map_err(other_error!(e, "failed parse ipv6 network"))?;
                    if network.prefix() > 0 {
                        request = request.source_prefix(network.ip(), network.prefix());
                    } else {
                        request
                            .message_mut()
                            .attributes
                            .push(RouteAttribute::PrefSource(RouteAddress::from(network.ip())));
                    }
                }

                if !route.gateway.is_empty() {
                    let ip = Ipv6Addr::from_str(&route.gateway)
                        .map_err(other_error!(e, "failed parse ipv6 network"))?;
                    request = request.gateway(ip);
                }

                if let Err(rtnetlink::Error::NetlinkError(message)) = request.execute().await {
                    if let Some(code) = message.code {
                        if Errno::from_raw(i32::from(code.abs())) != Errno::EEXIST {
                            return Err(other!(
                                "Failed to add IP v6 route (src: {}, dst: {}, gtw: {},Err: {})",
                                route.source,
                                route.dest,
                                route.gateway,
                                message
                            ));
                        }
                    }
                }
            } else {
                let dest_addr = if !route.dest.is_empty() {
                    Ipv4Network::from_str(&route.dest)
                        .map_err(other_error!(e, "failed to parse ipv4 network"))?
                } else {
                    Ipv4Network::new(Ipv4Addr::new(0, 0, 0, 0), 0)
                        .map_err(other_error!(e, "failed to new ipv4 network"))?
                };

                // Build IP v4 request
                let mut request = request
                    .v4()
                    .destination_prefix(dest_addr.ip(), dest_addr.prefix())
                    .output_interface(index);

                if !route.source.is_empty() {
                    let network = Ipv4Network::from_str(&route.source)
                        .map_err(other_error!(e, "failed to parse ipv4 network"))?;
                    if network.prefix() > 0 {
                        request = request.source_prefix(network.ip(), network.prefix());
                    } else {
                        request
                            .message_mut()
                            .attributes
                            .push(RouteAttribute::PrefSource(RouteAddress::from(network.ip())));
                    }
                }

                if !route.gateway.is_empty() {
                    let ip = Ipv4Addr::from_str(&route.gateway).map_err(other_error!(
                        e,
                        format!("failed to parse gateway({})", route.gateway)
                    ))?;
                    request = request.gateway(ip);
                }

                if let Err(rtnetlink::Error::NetlinkError(message)) = request.execute().await {
                    if let Some(code) = message.code {
                        if Errno::from_raw(i32::from(code.abs())) != Errno::EEXIST {
                            return Err(other!(
                                "Failed to add IP v4 route (src: {}, dst: {}, gtw: {},Err: {})",
                                route.source,
                                route.dest,
                                route.gateway,
                                message
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn delete_routes<I>(&mut self, routes: I) -> Result<()>
    where
        I: IntoIterator<Item = RouteMessage>,
    {
        for route in routes.into_iter() {
            if route.header.protocol == RouteProtocol::Kernel {
                continue;
            }

            let index = route
                .attributes
                .iter()
                .find_map(|attr| {
                    if let RouteAttribute::Oif(v) = attr {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .unwrap_or_default();

            let link = self.find_link(LinkFilter::Index(index)).await?;

            let name = link.name();
            if name.contains("lo") || name.contains("::1") {
                continue;
            }

            self.handle
                .route()
                .del(route)
                .execute()
                .await
                .map_err(other_error!(e, "failed to delete routes"))?;
        }

        Ok(())
    }
}

use netlink_packet_route::route::RouteFlag;

// netlink-packet-route-0.19.0/src/route/flags.rs:42
const ALL_ROUTE_FLAGS: [RouteFlag; 16] = [
    RouteFlag::Dead,
    RouteFlag::Pervasive,
    RouteFlag::Onlink,
    RouteFlag::Offload,
    RouteFlag::Linkdown,
    RouteFlag::Unresolved,
    RouteFlag::Trap,
    RouteFlag::Notify,
    RouteFlag::Cloned,
    RouteFlag::Equalize,
    RouteFlag::Prefix,
    RouteFlag::LookupTable,
    RouteFlag::FibMatch,
    RouteFlag::RtOffload,
    RouteFlag::RtTrap,
    RouteFlag::OffloadFailed,
];

// netlink-packet-route-0.19.0/src/route/flags.rs:87
pub(crate) struct VecRouteFlag(pub(crate) Vec<RouteFlag>);

impl From<u32> for VecRouteFlag {
    fn from(d: u32) -> Self {
        let mut got: u32 = 0;
        let mut ret = Vec::new();
        for flag in ALL_ROUTE_FLAGS {
            if (d & (u32::from(flag))) > 0 {
                ret.push(flag);
                got += u32::from(flag);
            }
        }
        if got != d {
            ret.push(RouteFlag::Other(d - got));
        }
        Self(ret)
    }
}

/// Minimal link view for the fork interface setter (index/name/MAC/UP).
/// This does not replace general netlink dumps. Ownership closes the query FD
/// on all paths; the final index check rejects a concurrent name reassignment.
fn fork_link_by_name(name: &str) -> std::io::Result<Link> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::io::{Error, ErrorKind};
    if name.is_empty() || name.len() >= libc::IFNAMSIZ || name.as_bytes().contains(&0) {
        return Err(Error::new(ErrorKind::InvalidInput, "invalid NIC name"));
    }
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 { return Err(Error::last_os_error()); }
    // SAFETY: socket returned a new owned descriptor.
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut req: libc::ifreq = unsafe { std::mem::zeroed() };
    for (out, byte) in req.ifr_name.iter_mut().zip(name.bytes()) { *out = byte as libc::c_char; }
    let mut query = |op| -> std::io::Result<libc::ifreq> {
        // SAFETY: req is a correctly sized, initialized Linux ifreq.
        if unsafe { libc::ioctl(fd.as_raw_fd(), op, &mut req) } < 0 {
            Err(Error::last_os_error())
        } else { Ok(req) }
    };
    let index = unsafe { query(libc::SIOCGIFINDEX)?.ifr_ifru.ifru_ifindex };
    let address = unsafe { query(libc::SIOCGIFHWADDR)?.ifr_ifru.ifru_hwaddr };
    if index <= 0 || address.sa_family != libc::ARPHRD_ETHER as libc::sa_family_t {
        return Err(Error::new(ErrorKind::InvalidData, "not an Ethernet NIC"));
    }
    let flags = unsafe { query(libc::SIOCGIFFLAGS)?.ifr_ifru.ifru_flags };
    if unsafe { query(libc::SIOCGIFINDEX)?.ifr_ifru.ifru_ifindex } != index {
        return Err(Error::new(ErrorKind::InvalidData, "NIC name changed during lookup"));
    }
    let mut message = LinkMessage::default();
    message.header.index = index as u32;
    if i32::from(flags) & libc::IFF_UP != 0 { message.header.flags.push(LinkFlag::Up); }
    message.attributes = vec![LinkAttribute::IfName(name.to_owned()),
        LinkAttribute::Address(address.sa_data[..6].iter().map(|x| *x as u8).collect())];
    Ok(message.into())
}

/// Local wrapper for netlink conversions and accessors.
pub(crate) struct Link(LinkMessage);

impl Link {
    /// If name.
    fn name(&self) -> String {
        self.0
            .attributes
            .iter()
            .find_map(|n| {
                if let LinkAttribute::IfName(name) = n {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    /// Returns whether the link is UP
    fn is_up(&self) -> bool {
        self.header.flags.contains(&LinkFlag::Up)
    }

    pub(crate) fn index(&self) -> u32 {
        self.header.index
    }
}

impl From<LinkMessage> for Link {
    fn from(msg: LinkMessage) -> Self {
        Link(msg)
    }
}

impl Deref for Link {
    type Target = LinkMessage;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct Address(AddressMessage);

impl TryFrom<Address> for IPAddress {
    type Error = containerd_shim::error::Error;

    fn try_from(value: Address) -> Result<Self> {
        let family = if value.is_ipv6() {
            IPFamily::v4
        } else {
            IPFamily::v6
        };

        let mut address = value.address();
        if address.is_empty() {
            address = value.local();
        }

        let mask = format!("{}", value.0.header.prefix_len);

        Ok(IPAddress {
            family: EnumOrUnknown::from(family),
            address,
            mask,
            ..IPAddress::default()
        })
    }
}

impl Address {
    fn is_ipv6(&self) -> bool {
        self.0.header.family == AddressFamily::Inet6
    }

    fn address(&self) -> String {
        self.0
            .attributes
            .iter()
            .find_map(|n| {
                if let AddressAttribute::Address(data) = n {
                    Some(data.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    fn local(&self) -> String {
        self.0
            .attributes
            .iter()
            .find_map(|n| {
                if let AddressAttribute::Local(data) = n {
                    Some(data.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }
}

fn parse_mac_address(addr: &str) -> Result<[u8; 6]> {
    let mut split = addr.splitn(6, ':');

    // Parse single Mac address block
    let mut parse_next = || -> Result<u8> {
        let v = u8::from_str_radix(
            split
                .next()
                .ok_or_else(|| other!("Invalid MAC address {}", addr))?,
            16,
        )?;
        Ok(v)
    };

    // Parse all 6 blocks
    let arr = [
        parse_next()?,
        parse_next()?,
        parse_next()?,
        parse_next()?,
        parse_next()?,
        parse_next()?,
    ];

    Ok(arr)
}

#[cfg(test)]
mod fork_network_tests {
    use super::*;

    #[test]
    fn fork_lookup_closes_temporary_descriptors() {
        let count = || std::fs::read_dir("/proc/self/fd").unwrap().count();
        let before = count();
        for _ in 0..100 {
            assert!(fork_link_by_name("missing-fork0").is_err());
            assert!(fork_link_by_name("").is_err());
            assert!(fork_link_by_name("lo").is_err()); // non-Ethernet fallback
        }
        assert_eq!(count(), before);
    }

    // Run only in an isolated network namespace with CAP_NET_ADMIN.
    #[tokio::test]
    #[ignore = "requires a private network namespace"]
    async fn fork_network_identity_and_route_dump() {
        let mut h = Handle::new().unwrap();
        h.handle.link().add().dummy("fork-test".into()).execute().await.unwrap();
        let index = h.find_link(LinkFilter::Name("fork-test")).await.unwrap().index();
        let old = "02:00:00:00:01:01";
        let new = "02:00:00:00:01:02";
        h.handle.link().set(index).address(parse_mac_address(old).unwrap().to_vec()).up().execute().await.unwrap();
        h.add_addresses(index, ["192.0.2.2/24".parse::<IpNetwork>().unwrap()]).await.unwrap();
        let intf = Interface {
            name: "fork-test".into(), hwAddr: new.into(), mtu: 1500,
            IPAddresses: vec![
                IPAddress { address: "198.51.100.2".into(), mask: "24".into(), ..Default::default() },
                IPAddress { address: "fd00::2".into(), mask: "64".into(), ..Default::default() },
            ], ..Default::default()
        };
        let fast = h.find_fork_interface(old, &[intf.clone()], true).await.unwrap();
        assert_eq!(fast.index(), index);
        h.apply_fork_interface(vec![intf.clone()], fast).await.unwrap();
        let link = h.find_link(LinkFilter::Name("fork-test")).await.unwrap();
        assert_eq!(link.index(), index);
        assert!(link.is_up());
        assert!(link.attributes.iter().any(|a| matches!(a, LinkAttribute::Address(mac) if mac.as_slice()==parse_mac_address(new).unwrap())));
        let addresses = h.list_addresses(AddressFilter::LinkIndex(index)).await.unwrap();
        assert!(addresses.iter().any(|a| a.address()=="198.51.100.2"));
        assert!(!addresses.iter().any(|a| a.address()=="192.0.2.2"));
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                let routes = h.query_routes(None).await.unwrap();
                if routes.iter().any(|r| r.header.address_family == AddressFamily::Inet)
                    && routes.iter().any(|r| r.header.address_family == AddressFamily::Inet6) { break; }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }).await.unwrap();
        // Reuse the validated NIC index for route installation, then verify
        // the kernel installed the route on precisely that device. Repeat to
        // cover deletion/reinstallation after a lost response.
        let route = Route { device: "fork-test".into(), gateway: "198.51.100.1".into(),
            family: IPFamily::v4.into(), ..Default::default() };
        for _ in 0..2 {
            h.apply_fork_routes(vec![route.clone()], "fork-test".into(), index).await.unwrap();
            assert!(h.query_routes(Some(IpVersion::V4)).await.unwrap().iter().any(|r|
                r.header.destination_prefix_length == 0 &&
                r.attributes.contains(&RouteAttribute::Oif(index)) &&
                r.attributes.contains(&RouteAttribute::Gateway(RouteAddress::Inet("198.51.100.1".parse().unwrap())))));
        }
        // A retried request still names the old MAC. It must remain idempotent.
        h.reset_fork_interface(old, vec![intf.clone()]).await.unwrap();
        // Preserve the MAC-based fallback when the guest renamed the interface.
        h.handle.link().set(index).name("renamed".into()).execute().await.unwrap();
        let renamed = h.find_fork_interface(old, &[intf.clone()], true).await.unwrap();
        h.apply_fork_interface(vec![intf], renamed).await.unwrap();
        assert_eq!(h.find_link(LinkFilter::Name("fork-test")).await.unwrap().index(), index);
    }
}
