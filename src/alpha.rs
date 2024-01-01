use futures::StreamExt;
use futures::{Stream, TryStream};
use std::cmp::max;
use std::future::ready;
use thiserror::Error;

#[derive(Clone)]
struct Alpha<V> {
    last_round_entered: Round,
    value: Option<Value<V>>,
}

impl<V> Alpha<V>
where
    V: Clone,
{
    async fn alpha<P>(&mut self, peers: &P, round: Round, value: V) -> Result<Option<V>, Error>
    where
        P: WriteClient<V> + ReadClient<V> + Quorum,
    {
        let value = match self.stage1(peers, round, value).await? {
            None => return Ok(None),
            Some(v) => v,
        };

        self.stage2(peers, round, value).await
    }

    async fn stage1<P>(&self, peers: &P, round: Round, value: V) -> Result<Option<V>, Error>
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

    async fn stage2<P>(&mut self, peers: &P, round: Round, value: V) -> Result<Option<V>, Error>
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

enum Stage1Result<V> {
    Abort,
    Continue(Option<V>),
}

#[derive(Clone, Debug)]
struct Value<V> {
    value: V,
    last_round_with_write: Round,
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct Round {
    tick: Tick,
    process_id: Id,
}

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct Id(u64);

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct Tick(u64);

#[derive(Error, Debug)]
enum Error {
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

trait ReadClient<V> {
    type Error: Into<Error>;
    type Stream: Stream<Item = Result<ReadResponse<V>, Self::Error>>;
    fn broadcast_read(&self, round: Round) -> Self::Stream;
}

trait WriteClient<V> {
    type Error: Into<Error>;
    type Stream: Stream<Item = Result<WriteResponse, Self::Error>>;
    fn broadcast_write(&self, value: Value<V>) -> Self::Stream;
}

trait Quorum {
    fn majority(&self) -> usize;
}