//! "arrangements" subcommand: cli tool to extract logical arrangement
//! sizes over time.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::DiagError;

use timely::dataflow::operators::{Filter, Map};

use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::reduce::Count;
use differential_dataflow::logging::DifferentialEvent;
use DifferentialEvent::{Merge, Batch};

use tdiag_connect::receive::ReplayWithShutdown;

/// @TODO
pub fn listen(
    timely_configuration: timely::Configuration,
    sockets: Vec<Option<std::net::TcpStream>>,) -> Result<(), crate::DiagError> {

    let sockets = Arc::new(Mutex::new(sockets));

    let is_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let is_running_w = is_running.clone();

    timely::execute(timely_configuration, move |worker| {
        let sockets = sockets.clone();

        // create replayer from disjoint partition of source worker identifiers.
        let replayer = tdiag_connect::receive::make_readers::<Duration, (Duration, timely::logging::WorkerIdentifier, DifferentialEvent)>(
            tdiag_connect::receive::ReplaySource::Tcp(sockets), worker.index(), worker.peers())
            .expect("failed to open tcp readers");
            
        worker.dataflow::<Duration, _, _>(|scope| {

            let window_size = Duration::from_secs(1);
            
            replayer.replay_with_shutdown_into(scope, is_running_w.clone())
                .filter(|(_, worker, _)| *worker == 0)
                .flat_map(|(t, _, x)| match x {
                    Batch(x) => Some((x.operator, t, x.length as isize)),
                    Merge(x) => {
                        match x.complete {
                            None => None,
                            Some(complete_size) => {
                                let size_diff = (complete_size as isize) - (x.length1 + x.length2) as isize;
                                
                                Some((x.operator, t, size_diff as isize))
                            }
                        }
                    }
                    _ => None,
                })
                .as_collection()
                .count()
                .delay(move |t| {
                    let w_secs = window_size.as_secs();

                    let secs_coarsened = if w_secs == 0 {
                        t.as_secs()
                    } else {
                        (t.as_secs() / w_secs + 1) * w_secs
                    };

                    Duration::new(secs_coarsened, 0)
                })
                .inspect(|x| println!("{:?}", x));
        })
    }).map_err(|x| DiagError(format!("error in the timely computation: {}", x)))?;

    Ok(())
}
