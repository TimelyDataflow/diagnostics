//! "profile" subcommand: reports aggregate runtime for each
//! scope/operator.

use std::sync::{Arc, Mutex};

use crate::{DiagError, LoggingTuple};

use timely::dataflow::operators::{Map, Filter, generic::Operator};

use differential_dataflow::trace::TraceReader;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::{Join, reduce::Threshold, Consolidate, arrange::{Arrange, Arranged}};

use timely::logging::TimelyEvent::{Operates, Schedule};

use tdiag_connect::receive::ReplayWithShutdown;
use timely::progress::frontier::AntichainRef;

/// Prints aggregate time spent in each scope/operator.
///
/// 1. Listens to incoming connections from a timely-dataflow program
/// with logging enabled;
/// 2. runs a differential-dataflow program to track scheduling events
/// and derive runtime for each operator;
/// 3. prints the resulting measurements alongside operator names and
/// scope names;
pub fn listen_and_profile(
    timely_configuration: timely::Config,
    sockets: Vec<Option<std::net::TcpStream>>) -> Result<(), crate::DiagError> {

    let sockets = Arc::new(Mutex::new(sockets));

    let (output_send, output_recv) = ::std::sync::mpsc::channel();
    let output_send = Arc::new(Mutex::new(output_send));

    let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let is_running_w = is_running.clone();

    let worker_handles = timely::execute(timely_configuration, move |worker| {
        let output_send: std::sync::mpsc::Sender<_> = output_send.lock().expect("cannot lock output_send").clone();

        let sockets = sockets.clone();

        // create replayer from disjoint partition of source worker identifiers.
        let replayer = tdiag_connect::receive::make_readers::<std::time::Duration, LoggingTuple>(
            tdiag_connect::receive::ReplaySource::Tcp(sockets), worker.index(), worker.peers())
            .expect("failed to open tcp readers");

        let profile_trace = worker.dataflow(|scope| {
            let stream = replayer.replay_with_shutdown_into(scope, is_running_w.clone());

            let operates = stream
                .filter(|(_, w, _)| *w== 0)
                .flat_map(|(t, _, x)| if let Operates(event) = x { Some((event, t, 1 as isize)) } else { None })
                .as_collection();

            let schedule = stream
                .flat_map(|(t, w, x)| if let Schedule(event) = x { Some((t, w, event)) } else { None })
                .unary(timely::dataflow::channels::pact::Pipeline, "Schedules", |_,_| {
                    let mut map = std::collections::HashMap::new();
                    let mut vec = Vec::new();
                    move |input, output| {
                        input.for_each(|time, data| {
                            data.swap(&mut vec);
                            let mut session = output.session(&time);
                            for (ts, worker, event) in vec.drain(..) {
                                let key = (worker, event.id);
                                match event.start_stop {
                                    timely::logging::StartStop::Start => {
                                        assert!(!map.contains_key(&key));
                                        map.insert(key, ts);
                                    },
                                    timely::logging::StartStop::Stop => {
                                        assert!(map.contains_key(&key));
                                        let end = map.remove(&key).unwrap();
                                        let ts_clip = std::time::Duration::from_secs(ts.as_secs() + 1);
                                        let elapsed = ts - end;
                                        let elapsed_ns = (elapsed.as_secs() as isize) * 1_000_000_000 + (elapsed.subsec_nanos() as isize);
                                        session.give((key.1, ts_clip, elapsed_ns));
                                    }
                                }
                            }
                        });
                    }
                }).as_collection().consolidate(); // (operator_id)

            // FIXME
            // == Re-construct the dataflow graph (re-wire channels crossing a scope boundary) ==
            //
            // A timely dataflow graph has a hierarchical structure: a "scope" looks like an
            // operator to the outside but can contain a subgraph of operators (and other scopes)
            //
            // We flatten this hierarchy to display it as a simple directed graph, but preserve the
            // information on scope boundaries so that they can be drawn as graph cuts.

            let operates = operates.map(|event| (event.addr, (event.id, event.name)));

            // Addresses of potential scopes (excluding leaf operators)
            let scopes = operates.map(|(mut addr, _)| {
                addr.pop();
                addr
            }).distinct();

            // Exclusively leaf operators
            let operates_without_subg = operates.antijoin(&scopes).map(|(addr, (id, name))| (id, (addr, name, false)));
            let subg = operates.semijoin(&scopes).map(|(addr, (id, name))| (id, (addr, name, true)));

            let all_operators = operates_without_subg.concat(&subg).distinct();

            use differential_dataflow::trace::implementations::ord::OrdKeySpine;
            let Arranged { trace: profile_trace, .. } = all_operators.semijoin(&schedule)
                .map(|(id, (addr, name, is_scope))| (id, addr, name, is_scope))
                .consolidate()
                .arrange::<OrdKeySpine<_, _, _>>();

            profile_trace
        });

        while worker.step() { }

        let mut profile_trace = profile_trace;

        profile_trace.set_physical_compaction(AntichainRef::new(&[]));

        let (mut cursor, storage) = profile_trace.cursor();

        use differential_dataflow::trace::cursor::Cursor;
        while cursor.key_valid(&storage) {
            let key = cursor.key(&storage);
            if cursor.val_valid(&storage) {
                let mut ns = 0;
                cursor.map_times(&storage, |_, r| ns += r);
                output_send.send((key.clone(), ns)).expect("failed to send output to mpsc channel");
            }
            cursor.step_key(&storage);
        }

    }).map_err(|x| DiagError(format!("error in the timely computation: {}", x)))?;

    {
        use std::io;
        use std::io::prelude::*;

        let mut stdin = io::stdin();
        let mut stdout = io::stdout();

        write!(stdout, "Press enter to stop collecting profile data (this will crash the source computation if it hasn't terminated).")
            .expect("failed to write to stdout");
        stdout.flush().unwrap();

        // Read a single byte and discard
        let _ = stdin.read(&mut [0u8]).expect("failed to read from stdin");
    }

    is_running.store(false, std::sync::atomic::Ordering::Release);

    worker_handles.join().into_iter().collect::<Result<Vec<_>, _>>().expect("Timely error");

    let mut data = output_recv.into_iter().collect::<Vec<_>>();
    data.sort_unstable_by_key(|&(_, ns)| std::cmp::Reverse(ns));
    for ((id, addr, name, is_scope), ns) in data.into_iter() {
        println!("{}\t{}\t(id={}, addr={:?}):\t{:e} s",
            if is_scope { "[scope]" } else { "" },
            name,
            id,
            addr,
            (ns as f64) / 1_000_000_000f64);
    }

    Ok(())
}
