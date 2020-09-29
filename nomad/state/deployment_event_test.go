package state

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func TestDeploymentEventFromChanges(t *testing.T) {
	t.Parallel()
	s := TestStateStoreCfg(t, TestStateStorePublisher(t))
	defer s.StopEventPublisher()

	// setup
	setupTx := s.db.WriteTxn(10)

	j := mock.Job()
	e := mock.Eval()
	e.JobID = j.ID

	d := mock.Deployment()
	d.JobID = j.ID

	require.NoError(t, s.upsertJobImpl(10, j, false, setupTx))
	require.NoError(t, s.upsertDeploymentImpl(10, d, setupTx))

	setupTx.Txn.Commit()

	ctx := context.WithValue(context.Background(), CtxMsgType, structs.DeploymentStatusUpdateRequestType)

	req := &structs.DeploymentStatusUpdateRequest{
		DeploymentUpdate: &structs.DeploymentStatusUpdate{
			DeploymentID:      d.ID,
			Status:            structs.DeploymentStatusPaused,
			StatusDescription: structs.DeploymentStatusDescriptionPaused,
		},
		Eval: e,
		// Exlude Job and assert its added
	}

	require.NoError(t, s.UpdateDeploymentStatus(ctx, 100, req))

	events := WaitForEvents(t, s, 100, 1, 1*time.Second)
	require.Len(t, events, 1)

	got := events[0]
	require.Equal(t, uint64(100), got.Index)
	require.Equal(t, d.ID, got.Key)

	de := got.Payload.(*DeploymentEvent)
	require.Equal(t, structs.DeploymentStatusPaused, de.Deployment.Status)
	require.Equal(t, j, de.Job)

}

func WaitForEvents(t *testing.T, s *StateStore, index uint64, want int, timeout time.Duration) []stream.Event {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			require.Fail(t, "timeout waiting for events")
		}
	}()

	maxAttempts := 10
	for {
		got := EventsForIndex(t, s, index)
		if len(got) == want {
			return got
		}
		maxAttempts--
		if maxAttempts == 0 {
			require.Fail(t, "reached max attempts waiting for desired event count")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func EventsForIndex(t *testing.T, s *StateStore, index uint64) []stream.Event {
	pub, err := s.EventPublisher()
	require.NoError(t, err)

	sub, err := pub.Subscribe(&stream.SubscribeRequest{
		Topics: map[stream.Topic][]string{
			"*": []string{"*"},
		},
		Index: index,
	})
	defer sub.Unsubscribe()

	require.NoError(t, err)

	var events []stream.Event
	for {
		e, err := sub.NextNoBlock()
		require.NoError(t, err)
		if e == nil {
			break
		}
		events = append(events, e...)
	}
	return events
}
