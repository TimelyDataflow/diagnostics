//! "arrangements" subcommand: cli tool to extract logical arrangement
//! sizes over time.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::convert::TryFrom;

use crate::DiagError;

use timely::dataflow::operators::{Filter, Map};
use timely::logging::{TimelyEvent, WorkerIdentifier};
use TimelyEvent::Operates;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::{Count, Join};
use DifferentialEvent::{Batch, Merge, MergeShortfall, TraceShare};

use tdiag_connect::receive::ReplayWithShutdown;

/// Prints the number of tuples maintained in each arrangement.
///
/// 1. Listens to incoming connections from a differential-dataflow
/// program with timely and differential logging enabled;
/// 2. runs a differential-dataflow program to track batching and
/// compaction events and derive number of tuples for each trace;
/// 3. prints the current size alongside arrangement names;
pub fn listen(
    timely_configuration: timely::Config,
    timely_sockets: Vec<Option<std::net::TcpStream>>,
    differential_sockets: Vec<Option<std::net::TcpStream>>,
    output_interval_ms: u64, 
) -> Result<(), crate::DiagError> {
    let timely_sockets = Arc::new(Mutex::new(timely_sockets));
    let differential_sockets = Arc::new(Mutex::new(differential_sockets));

    let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let is_running_w = is_running.clone();

    timely::execute(timely_configuration, move |worker| {
        let timely_sockets = timely_sockets.clone();
        let differential_sockets = differential_sockets.clone();

        let timely_replayer = tdiag_connect::receive::make_readers::<
            Duration,
            (Duration, WorkerIdentifier, TimelyEvent),
        >(
            tdiag_connect::receive::ReplaySource::Tcp(timely_sockets),
            worker.index(),
            worker.peers(),
        )
        .expect("failed to open timely tcp readers");

        let differential_replayer = tdiag_connect::receive::make_readers::<
            Duration,
            (Duration, WorkerIdentifier, DifferentialEvent),
        >(
            tdiag_connect::receive::ReplaySource::Tcp(differential_sockets),
            worker.index(),
            worker.peers(),
        )
            .expect("failed to open differential tcp readers");

        worker.dataflow::<Duration, _, _>(|scope| {
            let operates = timely_replayer
                .replay_with_shutdown_into(scope, is_running_w.clone())
                .flat_map(|(t, worker, x)| {
                    if let Operates(event) = x {
                        Some((
                            (
                                (worker, event.id),
                                format!("{} ({:?})", event.name, event.addr),
                            ),
                            t,
                            1 as isize,
                        ))
                    } else {
                        None
                    }
                })
                .as_collection();

            let events =
                differential_replayer.replay_with_shutdown_into(scope, is_running_w.clone());

            // Print output header.
            println!("ms\tWorker\tOp. Id\tName\t# of tuples");

            // Track sizes.
            events
                .flat_map(|(t, worker, x)| match x {
                    Batch(x) => Some(((worker, x.operator), t, x.length as isize)),
                    Merge(x) => match x.complete {
                        None => None,
                        Some(complete_size) => {
                            let size_diff =
                                (complete_size as isize) - (x.length1 + x.length2) as isize;

                            Some(((worker, x.operator), t, size_diff as isize))
                        }
                    },
                    MergeShortfall(x) => {
                        eprintln!("MergeShortfall {:?}", x);
                        None
                    },
                    DifferentialEvent::Drop(x) => Some(((worker, x.operator), t, -(x.length as isize))),
                    TraceShare(_x) => None,
                })
                .as_collection()
                .delay(move |t| {
                    let timestamp: u64 = u64::try_from(t.as_millis())
                        .expect("Why are the timestamps larger than humans are old?");

                    let window_idx = (timestamp / output_interval_ms) + 1;

                    Duration::from_millis(window_idx * output_interval_ms)
                })
                .count()
                .inner
                // We do not bother with retractions here, because the
                // user is only interested in the current count.
                .filter(|(_, _, count)| count >= &0)
                .as_collection()
                .join(&operates)
                .inspect(|(((worker, operator), (count, name)), t, _diff)| {
                    println!("{}\t{}\t{}\t{}\t{}", t.as_millis(), worker, operator, name, count);
                });
        })
    })
    .map_err(|x| DiagError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}
