use crate::alpha::{Alpha, Id, Quorum, ReadClient, Round, WriteClient};

struct Proposer<V, P, D> {
    id: Id,
    alpha: Alpha<V>,
    peers: P,
    failure_detector: D,
}

impl<V, P, D> Proposer<V, P, D>
where
    V: Clone,
    D: FailureDetector,
    P: WriteClient<V> + ReadClient<V> + Quorum,
{
    async fn propose(&mut self, value: V) -> V {
        let mut round = Round::new(self.id);

        loop {
            if self.failure_detector.leader() == self.id {
                if let Ok(Some(consensus)) =
                    self.alpha.alpha(&self.peers, round, value.clone()).await
                {
                    break consensus;
                }
                round = round.next();
            }
        }
    }
}

trait FailureDetector {
    fn leader(&self) -> Id;
}
