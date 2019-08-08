//! Command-line tools (with browser-based visualization)
//! to inspect a timely-dataflow computation.
//!
//! See the README and --help for usage information.

use tdiag::*;

fn run() -> Result<(), DiagError> {
    let args = clap::App::new("tdiag")
        .about(
"Diagostic tools for timely-dataflow programs.
Run the timely program to inspect with `env TIMELY_WORKER_LOG_ADDR=127.0.0.1:51317 cargo run ...`.
You can customize the interface and port for the receiver (this program) with --interface and --port.
"
        )
        .arg(clap::Arg::with_name("interface")
             .short("i")
             .long("interface")
             .value_name("INTERFACE")
             .help("Interface (ip address) to listen on; defaults to 127.0.0.1")
             .default_value("127.0.0.1")
             .takes_value(true))
        .arg(clap::Arg::with_name("port")
             .short("p")
             .long("port")
             .value_name("PORT")
             .help("Port to listen on; defaults to 51317")
             .default_value("51317")
             .required(true))
        .arg(clap::Arg::with_name("source_peers")
             .short("s")
             .long("source-peers")
             .value_name("PEERS")
             .help("Number of workers in the source computation")
             .required(true))
        .arg(clap::Arg::with_name("diag_workers")
             .short("w")
             .long("diag-workers")
             .value_name("WORKERS")
             .help("Number of worker threads for the diagnostic tool")
             .default_value("1"))
        .subcommand(clap::SubCommand::with_name("graph")
            .about("Render a computation's dataflow graph")
            .arg(clap::Arg::with_name("output_path")
                .short("o")
                .long("out")
                .value_name("PATH")
                .help("The output path for the generated html file (don't forget the .html extension)")
                .required(true))
        )
        .subcommand(
            clap::SubCommand::with_name("profile")
                .about("Print total time spent running each operator")
        )
        .subcommand(
            clap::SubCommand::with_name("differential")
                .about("Tools for profiling Timely computations that make use of differential dataflow.")
                .arg(clap::Arg::with_name("port")
                     .short("p")
                     .long("port")
                     .value_name("PORT")
                     .help("Port to listen on for Differential log streams; defaults to 51318")
                     .default_value("51318")
                     .required(true))
                .subcommand(
                    clap::SubCommand::with_name("arrangements")
                        .about("Track the logical size of arrangements over the course of a computation")
                        .help("
Add the following snippet to your Differential computation:

```
if let Ok(addr) = ::std::env::var(\"DIFFERENTIAL_LOG_ADDR\") {
    if let Ok(stream) = ::std::net::TcpStream::connect(&addr) {
        differential_dataflow::logging::enable(worker, stream);
        info!(\"enabled DIFFERENTIAL logging to {}\", addr);
    } else {
        panic!(\"Could not connect to differential log address: {:?}\", addr);
    }
}
```

Then start your computation with the DIFFERENTIAL_LOG_ADDR environment
variable pointing to tdiag's differential port (51318 by default).
")
                )
        )
        .get_matches();

    match args.subcommand() {
        (_, None) => Err(DiagError("Invalid subcommand".to_string()))?,
        _ => (),
    }

    let ip_addr: std::net::IpAddr = args.value_of("interface").expect("error parsing args")
        .parse().map_err(|e| DiagError(format!("Invalid --interface: {}", e)))?;
    let port: u16 = args.value_of("port").expect("error parsing args")
        .parse().map_err(|e| DiagError(format!("Invalid --port: {}", e)))?;
    let source_peers: usize = args.value_of("source_peers").expect("error parsing args")
        .parse().map_err(|e| DiagError(format!("Invalid --source-peers: {}", e)))?;
    let diag_workers: usize = args.value_of("diag_workers").expect("error parsing args")
        .parse().map_err(|e| DiagError(format!("Invalid --diag-workers: {}", e)))?;

    let timely_configuration = match diag_workers {
        1 => timely::Configuration::Thread,
        n => timely::Configuration::Process(n),
    };

    match args.subcommand() {
        ("graph", Some(graph_args)) => {
            let output_path = std::path::Path::new(graph_args.value_of("output_path").expect("error parsing args"));
            println!("Listening for {} connections on {}:{}", source_peers, ip_addr, port);
            let sockets = tdiag_connect::receive::open_sockets(ip_addr, port, source_peers)?;
            println!("Trace sources connected");
            crate::commands::graph::listen_and_render(timely_configuration, sockets, output_path)
        }
        ("profile", Some(_profile_args)) => {
            println!("Listening for {} connections on {}:{}", source_peers, ip_addr, port);
            let sockets = tdiag_connect::receive::open_sockets(ip_addr, port, source_peers)?;
            println!("Trace sources connected");
            crate::commands::profile::listen_and_profile(timely_configuration, sockets)
        }
        ("differential", Some(differential_args)) => {

            let differential_port: u16 = differential_args.value_of("port")
                .expect("error parsing args")
                .parse()
                .map_err(|e| DiagError(format!("Invalid --port: {}", e)))?;
            
            match differential_args.subcommand() {
                ("arrangements", Some(_args)) => {
                    use std::net::{TcpListener, ToSocketAddrs};
                    
                    let timely_listener = {
                        println!(
                            "Listening for {} Timely connections on {}:{}",
                            source_peers, ip_addr, port
                        );

                        let socket_addr = (ip_addr, port).to_socket_addrs()?
                            .next()
                            .ok_or(tdiag_connect::ConnectError::Other("Invalid listening address".to_string()))?;

                        TcpListener::bind(socket_addr)?
                    };

                    let differential_listener = {
                        println!(
                            "Listening for {} Differential connections on {}:{}",
                            source_peers,
                            ip_addr,
                            differential_port
                        );

                        let socket_addr = (ip_addr, differential_port).to_socket_addrs()?
                            .next()
                            .ok_or(tdiag_connect::ConnectError::Other("Invalid listening address".to_string()))?;

                        TcpListener::bind(socket_addr)?
                    };

                    let timely_sockets = (0..source_peers).map(|_| {
                        let socket = timely_listener.incoming().next().expect("Socket unexpectedly unavailable");
                        if let Ok(ref s) = &socket {
                            s.set_nonblocking(true)?;
                        }
                        socket.map(Some)
                    }).collect::<Result<Vec<_>, _>>()?;

                    let differential_sockets = (0..source_peers).map(|_| {
                        let socket = differential_listener.incoming().next().expect("Socket unexpectedly unavailable");
                        if let Ok(ref s) = &socket {
                            s.set_nonblocking(true)?;
                        }
                        socket.map(Some)
                    }).collect::<Result<Vec<_>, _>>()?;

                    println!("Trace sources connected");
                    crate::commands::arrangements::listen(
                        timely_configuration,
                        timely_sockets,
                        differential_sockets,
                    )
                }
                _ => panic!("Invalid subcommand for differential diagnostics"),
            }
        }
        _ => panic!("Invalid subcommand"),
    }
}

fn main() {
    match run() {
        Ok(()) => (),
        Err(DiagError(e)) => eprintln!("Error: {}", e),
    }
}
