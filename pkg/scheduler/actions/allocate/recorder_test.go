/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package allocate

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestNewRecorder(t *testing.T) {
	recorder := NewRecorder()
	if recorder == nil {
		t.Errorf("NewRecorder should not return nil")
	}
	if recorder.jobDecisions == nil {
		t.Errorf("recorder.jobDecisions should be initialized")
	}
	if recorder.podBunchDecisions == nil {
		t.Errorf("recorder.podBunchDecisions should be initialized")
	}
}

func TestSaveJobDecision(t *testing.T) {
	recorder := NewRecorder()
	jobID := api.JobID("job1")
	hyperNode := "node1"
	recorder.SaveJobDecision(jobID, hyperNode)

	if recorder.jobDecisions[jobID] != hyperNode {
		t.Errorf("SaveJobDecision should save the correct hyperNode for the job")
	}
}

func TestSavePodBunchDecision(t *testing.T) {
	recorder := NewRecorder()
	jobID := api.JobID("job1")
	hyperNodeForJob := "node1"
	podBunchID := api.BunchID("bunch1")
	hyperNodeForPodBunch := "node2"
	recorder.SavePodBunchDecision(jobID, hyperNodeForJob, podBunchID, hyperNodeForPodBunch)

	if recorder.podBunchDecisions[jobID][hyperNodeForJob][podBunchID] != hyperNodeForPodBunch {
		t.Errorf("SavePodBunchDecision should save the correct hyperNode for the podBunch")
	}
}

func TestUpdateDecisionToJob(t *testing.T) {
	jobID := api.JobID("job1")
	recorder := &Recorder{
		jobDecisions: map[api.JobID]string{
			jobID: "node2",
		},
	}

	job := &api.JobInfo{
		UID:                jobID,
		AllocatedHyperNode: "node1",
	}

	hyperNodes := map[string]*api.HyperNodeInfo{
		"node1": {
			Name:      "node1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string](),
		},
		"node2": {
			Name:      "node2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor2",
			Children:  sets.New[string](),
		},
		"ancestor1": {
			Name:      "ancestor1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "",
			Children:  sets.New[string]("node1"),
		},
		"ancestor2": {
			Name:      "ancestor2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string]("node2"),
		},
	}

	recorder.UpdateDecisionToJob(job, hyperNodes)

	if job.AllocatedHyperNode != "ancestor1" {
		t.Errorf("Job allocated hyperNode should be updated to ancestor1")
	}
}

func TestUpdateDecisionToJob_PodBunch(t *testing.T) {
	jobID := api.JobID("job1")
	hyperNodeForJob := "node2"
	podBunchID := api.BunchID("podBunch1")
	hyperNodeForPodBunch := "node2"
	recorder := &Recorder{
		jobDecisions: map[api.JobID]string{
			jobID: hyperNodeForJob,
		},
		podBunchDecisions: map[api.JobID]map[string]map[api.BunchID]string{
			jobID: {
				hyperNodeForJob: {
					podBunchID: hyperNodeForPodBunch,
				},
			},
		},
	}

	job := &api.JobInfo{
		UID:                jobID,
		AllocatedHyperNode: "node1",
		PodBunches: map[api.BunchID]*api.PodBunchInfo{
			podBunchID: {
				UID:                podBunchID,
				AllocatedHyperNode: "node1",
			},
		},
	}

	hyperNodes := map[string]*api.HyperNodeInfo{
		"node1": {
			Name:      "node1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string](),
		},
		"node2": {
			Name:      "node2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor2",
			Children:  sets.New[string](),
		},
		"ancestor1": {
			Name:      "ancestor1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "",
			Children:  sets.New[string]("node1"),
		},
		"ancestor2": {
			Name:      "ancestor2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string]("node2"),
		},
	}

	recorder.UpdateDecisionToJob(job, hyperNodes)

	if job.AllocatedHyperNode != "ancestor1" {
		t.Errorf("Job allocated hyperNode should be updated to ancestor1")
	}
	if job.PodBunches[podBunchID].AllocatedHyperNode != "ancestor1" {
		t.Errorf("UpdateDecisionToJob should update the allocated hyperNode for the podBunch")
	}
}

func TestUpdateDecisionToJob_PodBunchNotFound(t *testing.T) {
	recorder := NewRecorder()
	jobID := api.JobID("job1")
	hyperNodeForJob := "node1"
	podBunchID := api.BunchID("bunch1")
	hyperNodeForPodBunch := "node2"
	recorder.SavePodBunchDecision(jobID, hyperNodeForJob, podBunchID, hyperNodeForPodBunch)

	job := &api.JobInfo{
		UID:        jobID,
		PodBunches: map[api.BunchID]*api.PodBunchInfo{},
	}

	hyperNodes := api.HyperNodeInfoMap{
		"node1": &api.HyperNodeInfo{Name: "node1"},
		"node2": &api.HyperNodeInfo{Name: "node2"},
		"node3": &api.HyperNodeInfo{Name: "node3"},
	}

	recorder.UpdateDecisionToJob(job, hyperNodes)
	// Ensure no panic or error
}

func TestUpdateDecisionToJob_NoHyperNode(t *testing.T) {
	recorder := NewRecorder()
	jobID := api.JobID("job1")

	job := &api.JobInfo{
		UID:                jobID,
		AllocatedHyperNode: "node2",
	}

	hyperNodes := api.HyperNodeInfoMap{
		"node1": &api.HyperNodeInfo{Name: "node1"},
		"node2": &api.HyperNodeInfo{Name: "node2"},
		"node3": &api.HyperNodeInfo{Name: "node3"},
	}

	recorder.UpdateDecisionToJob(job, hyperNodes)

	if job.AllocatedHyperNode != "node2" {
		t.Errorf("UpdateDecisionToJob should not update the allocated hyperNode for the job without a recorder")
	}
}

func TestUpdateDecisionToJob_NoPodBunch(t *testing.T) {
	recorder := NewRecorder()
	jobID := api.JobID("job1")
	hyperNodeForJob := "node1"
	recorder.SaveJobDecision(jobID, hyperNodeForJob)

	job := &api.JobInfo{
		UID: jobID,
		PodBunches: map[api.BunchID]*api.PodBunchInfo{
			api.BunchID("bunch1"): {
				UID:                api.BunchID("bunch1"),
				AllocatedHyperNode: "node2",
			},
		},
	}

	hyperNodes := map[string]*api.HyperNodeInfo{
		"node1": {
			Name:      "node1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string](),
		},
		"node2": {
			Name:      "node2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor2",
			Children:  sets.New[string](),
		},
		"ancestor1": {
			Name:      "ancestor1",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "",
			Children:  sets.New[string]("node1"),
		},
		"ancestor2": {
			Name:      "ancestor2",
			HyperNode: &topologyv1alpha1.HyperNode{},
			Parent:    "ancestor1",
			Children:  sets.New[string]("node2"),
		},
	}

	recorder.UpdateDecisionToJob(job, hyperNodes)

	if job.PodBunches["bunch1"].AllocatedHyperNode != "node2" {
		t.Errorf("UpdateDecisionToJob should not update the allocated hyperNode for the podBunch without a recorder")
	}
}
