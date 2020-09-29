package state

import (
	"fmt"

	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
)

const (
	TopicDeployment stream.Topic = "Deployment"
)

type DeploymentEvent struct {
	Deployment *structs.Deployment
	Job        *structs.Job        `json:",omitempty"`
	Eval       *structs.Evaluation `json:",omitempty"`
}

func DeploymentEventFromChanges(tx ReadTxn, changes Changes) ([]stream.Event, error) {

	var deployment *structs.Deployment
	var job *structs.Job
	var eval *structs.Evaluation

	for _, change := range changes.Changes {
		switch change.Table {
		case "deployment":
			after, ok := change.After.(*structs.Deployment)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Deployment")
			}

			deployment = after
		case "jobs":
			after, ok := change.After.(*structs.Job)
			if !ok {
				return nil, fmt.Errorf("transaction change was not a Job")
			}

			job = after
		case "evals":
			after, ok := change.After.(*structs.Evaluation)
			if !ok {
				return nil, fmt.Errorf("transaction change was not an Evaluation")
			}

			eval = after
		}
	}

	// grab latest job if it was not touched
	if job == nil {
		j, err := tx.First("jobs", "id", deployment.Namespace, deployment.JobID)
		if err != nil {
			return nil, fmt.Errorf("retrieving job for deployment event: %w", err)
		}
		if j != nil {
			job = j.(*structs.Job)
		}
	}

	event := stream.Event{
		Topic: TopicDeployment,
		Index: changes.Index,
		Key:   deployment.ID,
		Payload: &DeploymentEvent{
			Deployment: deployment,
			Job:        job,
			Eval:       eval,
		},
	}

	return []stream.Event{event}, nil
}
