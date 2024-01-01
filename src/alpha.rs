use futures::Stream;
use futures::StreamExt;
use std::cmp::max;
use std::fmt::Debug;
use std::future::ready;
use thiserror::Error;

#[derive(Clone)]
pub struct Alpha<V> {
    last_round_entered: Round,
    value: Option<Value<V>>,
}

impl<V> Alpha<V>
where
    V: Clone,
{
    pub async fn alpha<P>(&mut self, peers: &P, round: Round, value: V) -> Result<Option<V>, Error>
    where
        P: WriteClient<V> + ReadClient<V> + Quorum,
    {
        let value = match self.read_stage(peers, round, value).await? {
            None => return Ok(None),
            Some(v) => v,
        };

        self.write_stage(peers, round, value).await
    }

    async fn read_stage<P>(&self, peers: &P, round: Round, value: V) -> Result<Option<V>, Error>
    where
        P: WriteClient<V> + ReadClient<V> + Quorum,
    {
        let responses = peers
            .broadcast_read(round)
            .filter_map(|result| ready(result.ok()))
            .take(peers.majority())
            .collect::<Vec<ReadResponse<V>>>()
            .await;

        if responses.iter().any(|response| response.round > round) {
            return Ok(None);
        }

        let value = responses
            .into_iter()
            .max_by_key(|response| response.state.last_round_entered)
            .ok_or(Error::EmptyReadResponse)?
            .state
            .value
            .map(|v| v.value)
            .unwrap_or(value);

        Ok(Some(value))
    }

    async fn write_stage<P>(
        &mut self,
        peers: &P,
        round: Round,
        value: V,
    ) -> Result<Option<V>, Error>
    where
        P: WriteClient<V> + ReadClient<V> + Quorum,
    {
        self.last_round_entered = round;
        let new_value = Value {
            value,
            last_round_with_write: round,
        };
        self.value = Some(new_value.clone());

        let responses = peers
            .broadcast_write(new_value.clone())
            .filter_map(|result| ready(result.ok()))
            .take(peers.majority())
            .collect::<Vec<WriteResponse>>()
            .await;

        if responses
            .iter()
            .any(|response| response.last_round_entered > round)
        {
            return Ok(None);
        }

        Ok(Some(new_value.value))
    }

    fn read(&mut self, round: Round) -> ReadResponse<V> {
        self.last_round_entered = max(self.last_round_entered, round);
        ReadResponse {
            round,
            state: self.clone(),
        }
    }

    fn write(&mut self, value: Value<V>) -> WriteResponse {
        let round = value.last_round_with_write;

        if round >= self.last_round_entered
            && self
                .value
                .as_ref()
                .map_or(true, |v| round > v.last_round_with_write)
        {
            self.last_round_entered = round;
            self.value = Some(value);
        }
        WriteResponse {
            round,
            last_round_entered: self.last_round_entered,
        }
    }
}

#[derive(Clone, Debug)]
struct Value<V> {
    value: V,
    last_round_with_write: Round,
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Round {
    tick: Tick,
    process_id: Id,
}

impl Round {
    pub fn new(process_id: Id) -> Self {
        Self {
            tick: Tick::default(),
            process_id,
        }
    }

    pub fn next(self) -> Self {
        Self {
            tick: self.tick.next(),
            process_id: self.process_id,
        }
    }
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct Id(u64);

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Default)]
struct Tick(u64);

impl Tick {
    fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("no read response")]
    EmptyReadResponse,
}

struct ReadResponse<V> {
    round: Round,
    state: Alpha<V>,
}

struct WriteResponse {
    round: Round,
    last_round_entered: Round,
}

pub trait ReadClient<V> {
    type Error: Into<Error> + Debug;
    type Stream: Stream<Item = Result<ReadResponse<V>, Self::Error>>;
    fn broadcast_read(&self, round: Round) -> Self::Stream;
}

pub trait WriteClient<V> {
    type Error: Into<Error> + Debug;
    type Stream: Stream<Item = Result<WriteResponse, Self::Error>>;
    fn broadcast_write(&self, value: Value<V>) -> Self::Stream;
}

pub trait Quorum {
    fn majority(&self) -> usize;
}
