//! "arrangements" subcommand: cli tool to extract logical arrangement
//! sizes over time.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::DiagError;

use timely::dataflow::operators::{Filter, Map};
use timely::logging::{TimelyEvent, WorkerIdentifier};
use TimelyEvent::Operates;

use differential_dataflow::collection::AsCollection;
use differential_dataflow::logging::DifferentialEvent;
use differential_dataflow::operators::{Count, Join};
use DifferentialEvent::{Batch, Merge};

use tdiag_connect::receive::ReplayWithShutdown;

/// @TODO
pub fn listen(
    timely_configuration: timely::Configuration,
    timely_sockets: Vec<Option<std::net::TcpStream>>,
    differential_sockets: Vec<Option<std::net::TcpStream>>,
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
            let window_size = Duration::from_secs(1);

            let operates = timely_replayer
                .replay_with_shutdown_into(scope, is_running_w.clone())
                .filter(|(_, worker, _)| *worker == 0)
                .flat_map(|(t, _, x)| {
                    if let Operates(event) = x {
                        Some(((event.id, event.name), t, 1 as isize))
                    } else {
                        None
                    }
                })
                .as_collection();

            differential_replayer
                .replay_with_shutdown_into(scope, is_running_w.clone())
                .filter(|(_, worker, _)| *worker == 0)
                .flat_map(|(t, _, x)| match x {
                    Batch(x) => Some((x.operator, t, x.length as isize)),
                    Merge(x) => match x.complete {
                        None => None,
                        Some(complete_size) => {
                            let size_diff =
                                (complete_size as isize) - (x.length1 + x.length2) as isize;

                            Some((x.operator, t, size_diff as isize))
                        }
                    },
                    _ => None,
                })
                .as_collection()
                .count()
                .inner
                // We do not bother with retractions here, because the
                // user is only interested in the current count.
                .filter(|(_, _, count)| count >= &0)
                .as_collection()
                .delay(move |t| {
                    let w_secs = window_size.as_secs();

                    let secs_coarsened = if w_secs == 0 {
                        t.as_secs()
                    } else {
                        (t.as_secs() / w_secs + 1) * w_secs
                    };

                    Duration::new(secs_coarsened, 0)
                })
                .join(&operates)
                .inspect(|x| println!("{:?}", x));
        })
    })
    .map_err(|x| DiagError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}
