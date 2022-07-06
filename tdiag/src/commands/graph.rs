//! "graph" subcommand: browser-based tool to display the dataflow graph.

use std::sync::{Arc, Mutex};

use crate::{DiagError, LoggingTuple};

use timely::dataflow::operators::{Filter, capture::{Capture, extract::Extract}};
use timely::dataflow::operators::map::Map;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::{Join, reduce::Threshold, Consolidate};

use timely::logging::TimelyEvent::{Operates, Channels};

use tdiag_connect::receive::ReplayWithShutdown;

static GRAPH_HTML: &str = include_str!("graph/dataflow-graph.html");

/// Creates an html file that renders a timely dataflow graph.
///
/// 1. Listens to incoming connection from a timely-dataflow program with
/// logging enabled;
/// 2. runs a differential-dataflow program to reconstruct the source
/// computation's dataflow graph;
/// 3. writes an html file that renders the dataflow graph.
///
/// This module includes `graph/dataflow-graph.html` as a static resource.
pub fn listen_and_render(
    timely_configuration: timely::Config,
    sockets: Vec<Option<std::net::TcpStream>>,
    output_path: &std::path::Path) -> Result<(), crate::DiagError> {

    let sockets = Arc::new(Mutex::new(sockets));

    let (operators_send, operators_recv) = ::std::sync::mpsc::channel();
    let operators_send = Arc::new(Mutex::new(operators_send));

    let (channels_send, channels_recv) = ::std::sync::mpsc::channel();
    let channels_send = Arc::new(Mutex::new(channels_send));

    let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let is_running_w = is_running.clone();

    let worker_handles = timely::execute(timely_configuration, move |worker| {
        let operators_send: std::sync::mpsc::Sender<_> = operators_send.lock().expect("cannot lock operators_send").clone();
        let channels_send: std::sync::mpsc::Sender<_> = channels_send.lock().expect("cannot lock channels_send").clone();

        let sockets = sockets.clone();

        // create replayer from disjoint partition of source worker identifiers.
        let replayer = tdiag_connect::receive::make_readers::<std::time::Duration, LoggingTuple>(
            tdiag_connect::receive::ReplaySource::Tcp(sockets), worker.index(), worker.peers())
            .expect("failed to open tcp readers");

        worker.dataflow(|scope| {
            let stream = replayer.replay_with_shutdown_into(scope, is_running_w.clone())
                .filter(|(_, worker, _)| *worker == 0);

            let operates = stream
                .flat_map(|(t, _, x)| if let Operates(event) = x { Some((event, t, 1 as isize)) } else { None })
                .as_collection();

            let channels = stream
                .flat_map(|(t, _, x)| if let Channels(event) = x { Some((event, t, 1 as isize)) } else { None })
                .as_collection();

            // == Re-construct the dataflow graph (re-wire channels crossing a scope boundary) ==
            //
            // A timely dataflow graph has a hierarchical structure: a "scope" looks like an
            // operator to the outside but can contain a subgraph of operators (and other scopes)
            //
            // We flatten this hierarchy to display it as a simple directed graph, but preserve the
            // information on scope boundaries so that they can be drawn as graph cuts.

            let operates = operates.map(|event| (event.addr, event.name));

            // Addresses of potential scopes (excluding leaf operators)
            let scopes = operates.map(|(mut addr, _)| {
                addr.pop();
                addr
            }).distinct();

            // Exclusively leaf operators
            let operates_without_subg = operates.antijoin(&scopes);

            // Retain only subscopes that correspond to scopes observed in the logs (remove empty [] addrs)
            let subgraphs = operates.map(|(addr, _)| (addr, ())).semijoin(&scopes).map(|(addr, ())| addr);

            let channels = channels.map(|event| (event.id, (event.scope_addr, event.source, event.target)));

            // Output leaf operators
            {
                operates_without_subg
                    .consolidate()
                    .inner
                    .map(move |((addr, name), _, _)| (addr, name))
                    .capture_into(operators_send);
            }
            
            // Output channels
            {
                // Channels that enter a subscope (as seen from outside the subscope)
                // Their source is the operator representing the subscope
                let subg_channels_outside_ingress = channels
                    .map(|(id, (scope_addr, from, to))| {
                        let mut subscope_addr = scope_addr.clone();
                        subscope_addr.push(to.0);
                        (subscope_addr, (id, (scope_addr, from), to.1))
                    })
                    .semijoin(&subgraphs);

                // Channels that leave a subscope (as seen from outside the subscope)
                // Their destination is the operator representing the subscope
                let subg_channels_outside_egress = channels
                    .map(|(id, (scope_addr, from, to))| {
                        let mut subscope_addr = scope_addr.clone();
                        subscope_addr.push(from.0);
                        (subscope_addr, (id, from.1, (scope_addr, to)))
                    })
                    .semijoin(&subgraphs);

                // Join the external and internal representation of channels that enter a subscope
                //
                //                          subscope [0, 1, 4]
                //                        +-----------------------+
                //     ([0, 1], 3)        |       ([0, 1, 4], 1)  |
                //         o------------->|------------->o        |
                //               external | internal              |
                //                        +-----------------------+
                //
                // The external channel has addr [0, 1], source 3, destination 4 ([0, 1, 4] is the subscope).
                // The internal channel has addr [0, 1, 4], source 0 (special!), destination 1.
                let subg_ingress = subg_channels_outside_ingress
                    .map(|(subscope_addr, (id, orig, subscope_port))| ((subscope_addr, (0, subscope_port)), (id, orig)))
                    .join_map(
                        &channels.map(|(id, (scope_addr, from, to))| ((scope_addr, from), (id, to))),
                        |(scope_addr, _from), (id1, (orig_addr, orig_from)), (id2, to)| {
                            let mut orig_addr = orig_addr.clone();
                            orig_addr.push(orig_from.0);
                            let mut to_addr = scope_addr.clone();
                            to_addr.push(to.0);
                            (vec![*id1, *id2], true, orig_addr, to_addr, orig_from.1, to.1)
                        });

                // Join the external and internal representation of channels that leave a subscope
                //
                // The structure depicted above is inverted for channels leaving a subscope
                let subg_egress = subg_channels_outside_egress
                    .map(|(subscope_addr, (id, subscope_port, dest))| ((subscope_addr, (0, subscope_port)), (id, dest)))
                    .join_map(
                        &channels.map(|(id, (scope_addr, from, to))| ((scope_addr, to), (id, from))),
                        |(scope_addr, to), (id2, (dest_addr, dest_to)), (id1, from)| {
                            let mut from_addr = scope_addr.clone();
                            from_addr.push(from.0);
                            let mut dest_addr = dest_addr.clone();
                            dest_addr.push(dest_to.0);
                            (vec![*id1, *id2], true, from_addr, dest_addr, to.1, dest_to.1)
                        });

                // Select all other channels (those that don't enter/leave a subscope)
                let non_subg = channels
                    .map(|(id, (scope_addr, from, to))| {
                        let mut subscope_addr = scope_addr.clone();
                        subscope_addr.push(from.0);
                        (subscope_addr, (id, scope_addr, from, to))
                    })
                    .antijoin(&subgraphs)
                    .map(|(_, (id, scope_addr, from, to))| {
                        let mut subscope_addr = scope_addr.clone();
                        subscope_addr.push(to.0);
                        (subscope_addr, (id, scope_addr, from, to))
                    })
                    .antijoin(&subgraphs)
                    .map(|(_, (id, scope_addr, from, to))| {
                        let mut from_addr = scope_addr.clone();
                        from_addr.push(from.0);
                        let mut to_addr = scope_addr.clone();
                        to_addr.push(to.0);
                        (vec![id], false, from_addr, to_addr, from.1, to.1)
                    });

                subg_ingress
                    .concat(&subg_egress)
                    .concat(&non_subg)
                    .consolidate()
                    .inner
                    .map(|(x, _, _)| x)
                    .capture_into(channels_send);

            }
        })
    }).map_err(|x| DiagError(format!("error in the timely computation: {}", x)))?;

    {
        use std::io;
        use std::io::prelude::*;

        let mut stdin = io::stdin();
        let mut stdout = io::stdout();

        write!(stdout, "Press enter to generate graph (this will crash the source computation if it hasn't terminated).")
            .expect("failed to write to stdout");
        stdout.flush().unwrap();

        // Read a single byte and discard
        let _ = stdin.read(&mut [0u8]).expect("failed to read from stdin");
    }

    is_running.store(false, std::sync::atomic::Ordering::Release);

    worker_handles.join().into_iter().collect::<Result<Vec<_>, _>>().expect("Timely error");

    let mut file = std::fs::File::create(output_path).map_err(|e| DiagError(format!("io error: {}", e)))?;

    use std::io::Write;

    fn expect_write(e: Result<(), std::io::Error>) {
        e.expect("write failed");
    }

    expect_write(writeln!(file, "<body>"));
    expect_write(writeln!(file, "{}", GRAPH_HTML));
    expect_write(writeln!(file, "<script type=\"text/javascript\">"));

    expect_write(writeln!(file, "let operate = ["));
    for (addr, name) in operators_recv.extract().into_iter().flat_map(|(_t, v)| v) {
        expect_write(writeln!(
            file,
            "{{ \"name\": \"{}\", \"addr\": [{}] }},",
            name,
            addr.into_iter().map(|x| format!("{}, ", x)).collect::<Vec<_>>().concat()));
    }
    expect_write(writeln!(file, "];"));

    expect_write(writeln!(file, "let channel = ["));
    for (id, subgraph, from_addr, to_addr, from_port, to_port) in channels_recv.extract().into_iter().flat_map(|(_t, v)| v) {
        expect_write(writeln!(
            file,
            "{{ \"id\": [{}], \"subgraph\": {}, \"from_addr\": [{}], \"to_addr\": [{}], \"from_port\": {}, \"to_port\": {} }},",
            id.into_iter().map(|x| format!("{}, ", x)).collect::<Vec<_>>().concat(),
            subgraph,
            from_addr.into_iter().map(|x| format!("{}, ", x)).collect::<Vec<_>>().concat(),
            to_addr.into_iter().map(|x| format!("{}, ", x)).collect::<Vec<_>>().concat(),
            from_port,
            to_port));
    }
    expect_write(writeln!(file, "];"));

    expect_write(writeln!(file, "run(operate, channel);"));

    expect_write(writeln!(file, "</script>"));

    println!("Graph generated in file://{}", std::fs::canonicalize(output_path).expect("invalid path").to_string_lossy());

    Ok(())
}
