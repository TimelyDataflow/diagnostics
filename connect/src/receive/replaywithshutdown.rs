// This code adapted from https://github.com/TimelyDataflow/timely-dataflow/blob/master/timely/src/dataflow/operators/capture/replay.rs
//
// Timely Dataflow carries the following license:
//
// The MIT License (MIT)
//
// Copyright (c) 2014 Frank McSherry
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

use timely::dataflow::channels::pushers::{buffer::Buffer as PushBuffer, Counter as PushCounter};
use timely::dataflow::operators::generic::builder_raw::OperatorBuilder;
use timely::progress::frontier::MutableAntichain;
use timely::{
    dataflow::{Scope, Stream},
    progress::Timestamp,
    Data,
};

use timely::dataflow::operators::capture::event::{Event, EventIterator};

/// Replay a capture stream into a scope with the same timestamp.
pub trait ReplayWithShutdown<T: Timestamp, D: Data> {
    /// Replays `self` into the provided scope, as a `Stream<S, D>`.
    fn replay_with_shutdown_into<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D>;
}

impl<T: Timestamp, D: Data, I> ReplayWithShutdown<T, D> for I
where
    I: IntoIterator,
    <I as IntoIterator>::Item: EventIterator<T, D> + 'static,
{
    fn replay_with_shutdown_into<S: Scope<Timestamp = T>>(
        self,
        scope: &mut S,
        is_running: Arc<AtomicBool>,
    ) -> Stream<S, D> {
        let mut builder = OperatorBuilder::new("Replay".to_owned(), scope.clone());

        let address = builder.operator_info().address;
        let activator = scope.activator_for(&address[..]);

        let (targets, stream) = builder.new_output();

        let mut output = PushBuffer::new(PushCounter::new(targets));
        let mut event_streams = self.into_iter().collect::<Vec<_>>();
        let mut started = false;

        let mut antichain = MutableAntichain::new();

        builder.build(
            move |_frontier| {},
            move |_consumed, internal, produced| {
                if !started {
                    // The first thing we do is modify our capabilities to match the number of streams we manage.
                    // This should be a simple change of `self.event_streams.len() - 1`. We only do this once, as
                    // our very first action.
                    internal[0].update(Default::default(), (event_streams.len() as i64) - 1);
                    antichain.update_iter(
                        Some((Default::default(), (event_streams.len() as i64) - 1)).into_iter(),
                    );
                    started = true;
                }

                if is_running.load(Ordering::Acquire) {
                    for event_stream in event_streams.iter_mut() {
                        while let Some(event) = event_stream.next() {
                            match *event {
                                Event::Progress(ref vec) => {
                                    antichain.update_iter(vec.iter().cloned());
                                    internal[0].extend(vec.iter().cloned());
                                }
                                Event::Messages(ref time, ref data) => {
                                    output.session(time).give_iterator(data.iter().cloned());
                                }
                            }
                        }
                    }

                    // Always reschedule `replay`.
                    activator.activate();

                    output.cease();
                    output
                        .inner()
                        .produced()
                        .borrow_mut()
                        .drain_into(&mut produced[0]);
                } else {
                    while !antichain.is_empty() {
                        let elements = antichain
                            .frontier()
                            .iter()
                            .map(|t| (t.clone(), -1))
                            .collect::<Vec<_>>();
                        for (t, c) in elements.iter() {
                            internal[0].update(t.clone(), *c);
                        }
                        antichain.update_iter(elements);
                    }
                }

                false
            },
        );

        stream
    }
}
