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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	schedulingv1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	topologyv1alpha1 "volcano.sh/apis/pkg/apis/topology/v1alpha1"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/gang"
	networktopologyaware "volcano.sh/volcano/pkg/scheduler/plugins/network-topology-aware"
	"volcano.sh/volcano/pkg/scheduler/plugins/predicates"
	"volcano.sh/volcano/pkg/scheduler/uthelper"
	"volcano.sh/volcano/pkg/scheduler/util"
)

func lessFn(a, b interface{}) bool {
	return a.(int) < b.(int)
}

func TestShallowCopyFrom(t *testing.T) {
	w1 := &JobWorksheet{
		podBunches:         util.NewPriorityQueue(lessFn),
		podBunchWorksheets: make(map[api.BunchID]*PodBunchWorksheet),
	}
	w2 := &JobWorksheet{
		podBunches:         util.NewPriorityQueue(lessFn),
		podBunchWorksheets: make(map[api.BunchID]*PodBunchWorksheet),
	}
	w1.ShallowCopyFrom(w2)
	if !reflect.DeepEqual(w1, w2) {
		t.Errorf("Expected w1 to be equal to w2 after shallow copy, but they are not.")
	}

	p1 := &PodBunchWorksheet{
		tasks: util.NewPriorityQueue(lessFn),
	}
	p2 := &PodBunchWorksheet{
		tasks: util.NewPriorityQueue(lessFn),
	}
	p1.ShallowCopyFrom(p2)
	if !reflect.DeepEqual(p1, p2) {
		t.Errorf("Expected p1 to be equal to p2 after shallow copy, but they are not.")
	}
}

func TestEmpty(t *testing.T) {
	w := &JobWorksheet{
		podBunches:         nil,
		podBunchWorksheets: make(map[api.BunchID]*PodBunchWorksheet),
	}
	if !w.Empty() {
		t.Errorf("Expected w to be empty, but it is not.")
	}

	p := &PodBunchWorksheet{
		tasks: nil,
	}
	if !p.Empty() {
		t.Errorf("Expected p to be empty, but it is not.")
	}
}

func TestClone(t *testing.T) {
	w := &JobWorksheet{
		podBunches:         util.NewPriorityQueue(lessFn),
		podBunchWorksheets: make(map[api.BunchID]*PodBunchWorksheet),
	}
	wClone := w.Clone()
	if !cmp.Equal(w.podBunchWorksheets, wClone.podBunchWorksheets) || !cmp.Equal(w.podBunches.Pop(), wClone.podBunches.Pop()) {
		t.Errorf("Expected w and wClone to be equal after cloning, but they are not.")
	}

	w2 := &JobWorksheet{
		podBunches: util.NewPriorityQueue(lessFn),
		podBunchWorksheets: map[api.BunchID]*PodBunchWorksheet{
			"bunchId": {
				util.NewPriorityQueue(lessFn),
			},
		},
	}
	w2Clone := w2.Clone()
	if !cmp.Equal(w2.podBunchWorksheets["bunchId"].tasks.Pop(), w2Clone.podBunchWorksheets["bunchId"].tasks.Pop()) || !cmp.Equal(w2.podBunches.Pop(), w2Clone.podBunches.Pop()) {
		t.Errorf("Expected w2 and w2Clone to be equal after cloning, but they are not.")
	}

	p := &PodBunchWorksheet{
		tasks: util.NewPriorityQueue(lessFn),
	}
	pClone := p.Clone()
	if !cmp.Equal(p.tasks.Pop(), pClone.tasks.Pop()) {
		t.Errorf("Expected p and pClone to be equal after cloning, but they are not.")
	}
}

func TestAllocateWithPartitionPolicyNetworkTopology(t *testing.T) {
	plugins := map[string]framework.PluginBuilder{
		predicates.PluginName:           predicates.New,
		gang.PluginName:                 gang.New,
		networktopologyaware.PluginName: networktopologyaware.New,
	}

	tests := []uthelper.TestCommonStruct{
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain, can allocate job when resources are enough",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 3, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{0: sets.New[string]("s0", "s1"), 1: sets.New[string]("s2")},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			MinimalBindCheck: true,
			ExpectBindsNum:   3,
		},
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain, can allocate job when minavailable < replicas",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 1, nil, schedulingv1.PodGroupInqueue, "hard", 1,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain, two available hyperNodes, can allocate job to nodes with affinity",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 1, nil, schedulingv1.PodGroupInqueue, "hard", 1,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, map[string]string{"nodeRole": "master"}),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), map[string]string{"nodeRole": "master"}),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{0: sets.New[string]("s0", "s1")},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1"),
				"s1": sets.New[string]("s1-n2"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindMap: map[string]string{
				"c1/p1": "s1-n2",
			},
			ExpectBindsNum: 1,
		},
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain and tasks in job rescheduled, can allocate job when resources are enough",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s2", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s0-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain and tasks in job rescheduled, can allocate job when resources are enough and minavailable = replicas",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s2", "q1", 3, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s0-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch soft network topology constrain and tasks in job rescheduled, can allocate job when cross highestTierAllowed tier and hyperNodesInfo has three tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s1", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "soft", 0),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s3-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s3-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s3-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s3-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{
				1: sets.New[string]("s3", "s4", "s5", "s6"),
				2: sets.New[string]("s1", "s2"),
				3: sets.New[string]("s0")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 3, []api.MemberConfig{
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s2",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 2, []api.MemberConfig{
					{
						Name:     "s3",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s4",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s5",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s6",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s3": api.NewHyperNodeInfo(api.BuildHyperNode("s3", 1, []api.MemberConfig{
					{
						Name:     "s3-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s3-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s4": api.NewHyperNodeInfo(api.BuildHyperNode("s4", 1, []api.MemberConfig{
					{
						Name:     "s4-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s4-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s5": api.NewHyperNodeInfo(api.BuildHyperNode("s5", 1, []api.MemberConfig{
					{
						Name:     "s5-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s5-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s6": api.NewHyperNodeInfo(api.BuildHyperNode("s6", 1, []api.MemberConfig{
					{
						Name:     "s6-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s6-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2", "s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s1": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2"),
				"s2": sets.New[string]("s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s3": sets.New[string]("s3-n1", "s3-n2"),
				"s4": sets.New[string]("s4-n1", "s4-n2"),
				"s5": sets.New[string]("s5-n1", "s5-n2"),
				"s6": sets.New[string]("s6-n1", "s6-n2"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup soft network topology constrain and bunch hard network topology constrain, can allocate job when resources are enough",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 3, nil, schedulingv1.PodGroupInqueue, "soft", 0,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicyWithBunchSize("task1", []string{"volcano.sh/task-spec"}, "hard", 1, 2),
						util.BuildBunchPolicyWithBunchSize("task2", []string{"volcano.sh/task-instance"}, "hard", 1, 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-instance": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			MinimalBindCheck: true,
			ExpectBindsNum:   3,
			ExpectBindMap: map[string]string{
				"c1/p1": "s1-n3",
			},
		},
		{
			Name: "podgroup soft network topology constrain and bunch hard network topology constrain, can allocate job when minavailable < replicas",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 1, nil, schedulingv1.PodGroupInqueue, "soft", 0,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup soft network topology constrain and bunch hard network topology constrain, can not allocate job when cross highestTierAllowed tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 3, nil, schedulingv1.PodGroupInqueue, "soft", 0,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p4", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   0,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup soft network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can allocate job when resources are enough",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s2", "q1", 2, nil, schedulingv1.PodGroupInqueue, "soft", 0,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s1-n3", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum: 1,
			ExpectBindMap: map[string]string{
				"c1/p3": "s1-n4",
			},
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup soft network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can not allocate job when cross highestTierAllowed tier and hyperNodesInfo has three tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s1", "q1", 2, nil, schedulingv1.PodGroupInqueue, "soft", 0,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s3-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s3-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "s4-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p4", "s4-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p5", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s3-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s3-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{
				1: sets.New[string]("s3", "s4", "s5", "s6"),
				2: sets.New[string]("s1", "s2"),
				3: sets.New[string]("s0")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 3, []api.MemberConfig{
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s2",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 2, []api.MemberConfig{
					{
						Name:     "s3",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s4",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s5",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s6",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s3": api.NewHyperNodeInfo(api.BuildHyperNode("s3", 1, []api.MemberConfig{
					{
						Name:     "s3-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s3-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s4": api.NewHyperNodeInfo(api.BuildHyperNode("s4", 1, []api.MemberConfig{
					{
						Name:     "s4-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s4-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s5": api.NewHyperNodeInfo(api.BuildHyperNode("s5", 1, []api.MemberConfig{
					{
						Name:     "s5-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s5-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s6": api.NewHyperNodeInfo(api.BuildHyperNode("s6", 1, []api.MemberConfig{
					{
						Name:     "s6-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s6-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2", "s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s1": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2"),
				"s2": sets.New[string]("s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s3": sets.New[string]("s3-n1", "s3-n2"),
				"s4": sets.New[string]("s4-n1", "s4-n2"),
				"s5": sets.New[string]("s5-n1", "s5-n2"),
				"s6": sets.New[string]("s6-n1", "s6-n2"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   0,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain, can allocate job when highestTierAllowed not reached",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 3, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   3,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain, can allocate job according to the network topology constrain of bunch policy",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 3, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicyWithBunchSize("task1", []string{"volcano.sh/task-spec"}, "hard", 1, 1),
						util.BuildBunchPolicyWithBunchSize("task2", []string{"volcano.sh/task-instance"}, "hard", 1, 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-instance": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-instance": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum: 3,
			ExpectBindMap: map[string]string{
				"c1/p1": "s1-n3",
			},
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain, can allocate job when multi hyperNodes are available",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain, can allocate job when minavailable < replicas",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 1, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicyWithBunchSize("task1", []string{"volcano.sh/task-spec"}, "hard", 1, 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   2,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, two available hyperNodes, can allocate job to nodes with affinity",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s1-n3", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum: 1,
			ExpectBindMap: map[string]string{
				"c1/p3": "s1-n4",
			},
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can allocate job when highestTierAllowed not reached and minavailable = replicas",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s0", "q1", 3, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s0-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can allocate job when highestTierAllowed not reached and hyperNodesInfo has three tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s3", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 3,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s3-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s3-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s3-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s3-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{
				1: sets.New[string]("s3", "s4", "s5", "s6"),
				2: sets.New[string]("s1", "s2"),
				3: sets.New[string]("s0")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 3, []api.MemberConfig{
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s2",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 2, []api.MemberConfig{
					{
						Name:     "s3",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s4",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s5",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s6",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s3": api.NewHyperNodeInfo(api.BuildHyperNode("s3", 1, []api.MemberConfig{
					{
						Name:     "s3-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s3-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s4": api.NewHyperNodeInfo(api.BuildHyperNode("s4", 1, []api.MemberConfig{
					{
						Name:     "s4-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s4-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s5": api.NewHyperNodeInfo(api.BuildHyperNode("s5", 1, []api.MemberConfig{
					{
						Name:     "s5-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s5-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s6": api.NewHyperNodeInfo(api.BuildHyperNode("s6", 1, []api.MemberConfig{
					{
						Name:     "s6-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s6-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2", "s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s1": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2"),
				"s2": sets.New[string]("s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s3": sets.New[string]("s3-n1", "s3-n2"),
				"s4": sets.New[string]("s4-n1", "s4-n2"),
				"s5": sets.New[string]("s5-n1", "s5-n2"),
				"s6": sets.New[string]("s6-n1", "s6-n2"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can not allocate job when cross highestTierAllowed tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s0", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 2,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 1),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s0-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1"), 2: sets.New[string]("s2")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s0",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
				"s2": sets.New[string]("s0-n1", "s0-n2", "s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   0,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can not allocate job when cross highestTierAllowed tier and hyperNodesInfo has three tier",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s3", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 3,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s3-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s3-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "s4-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p4", "s4-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p5", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s3-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s3-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s4-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s5-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s6-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{
				1: sets.New[string]("s3", "s4", "s5", "s6"),
				2: sets.New[string]("s1", "s2"),
				3: sets.New[string]("s0")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 3, []api.MemberConfig{
					{
						Name:     "s1",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s2",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 2, []api.MemberConfig{
					{
						Name:     "s3",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s4",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s2": api.NewHyperNodeInfo(api.BuildHyperNode("s2", 2, []api.MemberConfig{
					{
						Name:     "s5",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
					{
						Name:     "s6",
						Type:     topologyv1alpha1.MemberTypeHyperNode,
						Selector: "exact",
					},
				})),
				"s3": api.NewHyperNodeInfo(api.BuildHyperNode("s3", 1, []api.MemberConfig{
					{
						Name:     "s3-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s3-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s4": api.NewHyperNodeInfo(api.BuildHyperNode("s4", 1, []api.MemberConfig{
					{
						Name:     "s4-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s4-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s5": api.NewHyperNodeInfo(api.BuildHyperNode("s5", 1, []api.MemberConfig{
					{
						Name:     "s5-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s5-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s6": api.NewHyperNodeInfo(api.BuildHyperNode("s6", 1, []api.MemberConfig{
					{
						Name:     "s6-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s6-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2", "s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s1": sets.New[string]("s3-n1", "s3-n2", "s4-n1", "s4-n2"),
				"s2": sets.New[string]("s5-n1", "s5-n2", "s6-n1", "s6-n2"),
				"s3": sets.New[string]("s3-n1", "s3-n2"),
				"s4": sets.New[string]("s4-n1", "s4-n2"),
				"s5": sets.New[string]("s5-n1", "s5-n2"),
				"s6": sets.New[string]("s6-n1", "s6-n2"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   0,
			MinimalBindCheck: true,
		},
		{
			Name: "podgroup hard network topology constrain and bunch hard network topology constrain and tasks in job rescheduled, can allocate job when LCAHyperNode is empty",
			PodGroups: []*schedulingv1.PodGroup{
				util.BuildPodGroupWithBunchPolicy("pg1", "c1", "s0", "q1", 2, nil, schedulingv1.PodGroupInqueue, "hard", 3,
					[]schedulingv1.BunchPolicySpec{
						util.BuildBunchPolicy("task1", []string{"volcano.sh/task-spec"}, "hard", 2),
					}),
			},
			Pods: []*v1.Pod{
				// should use different role, because allocate actions default to enable the role caches when predicate
				util.BuildPod("c1", "p1", "s0-n1", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "master"}, nil),
				util.BuildPod("c1", "p2", "s0-n2", v1.PodRunning, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
				util.BuildPod("c1", "p3", "", v1.PodPending, api.BuildResourceList("2", "4G"), "pg1", map[string]string{"volcano.sh/task-spec": "worker"}, nil),
			},
			Nodes: []*v1.Node{
				util.BuildNode("s0-n1", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s0-n2", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n3", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
				util.BuildNode("s1-n4", api.BuildResourceList("2", "4Gi", []api.ScalarResource{{Name: "pods", Value: "10"}}...), nil),
			},
			HyperNodesSetByTier: map[int]sets.Set[string]{1: sets.New[string]("s0", "s1")},
			HyperNodesMap: map[string]*api.HyperNodeInfo{
				"s0": api.NewHyperNodeInfo(api.BuildHyperNode("s0", 1, []api.MemberConfig{
					{
						Name:     "s0-n1",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s0-n2",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
				"s1": api.NewHyperNodeInfo(api.BuildHyperNode("s1", 1, []api.MemberConfig{
					{
						Name:     "s1-n3",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
					{
						Name:     "s1-n4",
						Type:     topologyv1alpha1.MemberTypeNode,
						Selector: "exact",
					},
				})),
			},
			HyperNodes: map[string]sets.Set[string]{
				"s0": sets.New[string]("s0-n1", "s0-n2"),
				"s1": sets.New[string]("s1-n3", "s1-n4"),
			},
			Queues: []*schedulingv1.Queue{
				util.BuildQueue("q1", 1, nil),
			},
			ExpectBindsNum:   1,
			MinimalBindCheck: true,
		},
	}

	trueValue := true
	tiers := []conf.Tier{
		{
			Plugins: []conf.PluginOption{
				{
					Name:                 gang.PluginName,
					EnabledJobOrder:      &trueValue,
					EnabledJobReady:      &trueValue,
					EnabledJobPipelined:  &trueValue,
					EnabledJobStarving:   &trueValue,
					EnabledPodBunchReady: &trueValue,
				},
				{
					Name:             predicates.PluginName,
					EnabledPredicate: &trueValue,
				},
				{
					Name:                     networktopologyaware.PluginName,
					EnabledNodeOrder:         &trueValue,
					EnabledHyperNodeOrder:    &trueValue,
					EnabledHyperNodeGradient: &trueValue,
				},
			},
		},
	}
	for i, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			test.Plugins = plugins
			test.RegisterSession(tiers, nil)
			defer test.Close()
			test.Run([]framework.Action{New()})
			if err := test.CheckAll(i); err != nil {
				t.Fatal(err)
			}
		})
	}
}
