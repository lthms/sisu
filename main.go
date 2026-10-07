package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var agentRegexp = regexp.MustCompile(`^agent-(0|[1-9][0-9]*)-[0-9a-f]{8}$`)
var controllerRegexp = regexp.MustCompile(`^control-plane-[0-9a-f]{8}$`)

func nodesByKind(ctx context.Context, k8s client.Client) (map[string][]node, error) {
	var nodes corev1.NodeList
	if err := k8s.List(ctx, &nodes); err != nil {
		return nil, fmt.Errorf("could not list nodes: %w", err)
	}

	byKind := make(map[string][]node)
	for i := range nodes.Items {
		parsed, err := New(&nodes.Items[i])
		if err != nil {
			continue
		}
		byKind[parsed.Kind] = append(byKind[parsed.Kind], *parsed)
	}

	return byKind, nil
}

func zombieNodes(byKind map[string][]node) []node {
	var others []node
	for kind, group := range byKind {
		if len(group) < 2 {
			continue
		}

		readyIndex := -1
		readyCount := 0
		for i, candidate := range group {
			for _, condition := range candidate.Node.Status.Conditions {
				if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
					readyIndex = i
					readyCount++
					break
				}
			}
		}

		if readyCount > 1 {
			slog.Error("multiple Ready nodes for kind", "kind", kind, "ready_count", readyCount)
			continue
		}
		if readyCount == 0 {
			continue
		}
		for i, candidate := range group {
			if i != readyIndex {
				others = append(others, candidate)
			}
		}
	}
	return others
}

func (n *node) deleteVolumeAttachments(ctx context.Context, k8s client.Client) error {
	var attachments storagev1.VolumeAttachmentList
	if err := k8s.List(ctx, &attachments); err != nil {
		return fmt.Errorf("could not list volume attachments: %w", err)
	}

	for i := range attachments.Items {
		attachment := &attachments.Items[i]
		if attachment.Spec.NodeName != n.Node.Name {
			continue
		}
		if err := k8s.Delete(ctx, attachment); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("could not delete volume attachment %q for node %q: %w", attachment.Name, n.Node.Name, err)
		}
	}

	return nil
}

// RESP: The reconciler now calls deleteZombies for every node event, including
// deletion events. Cleanup errors are returned to trigger retries.
func deleteZombies(ctx context.Context, k8s client.Client) error {
	byKind, err := nodesByKind(ctx, k8s)
	if err != nil {
		return err
	}

	for _, zombie := range zombieNodes(byKind) {
		if err := zombie.deleteVolumeAttachments(ctx, k8s); err != nil {
			return err
		}
		if err := k8s.Delete(ctx, zombie.Node); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("could not delete zombie node %q: %w", zombie.Node.Name, err)
		}
	}

	return nil
}

type node struct {
	Kind string
	Node *corev1.Node
}

func New(k8sNode *corev1.Node) (*node, error) {
	if matches := agentRegexp.FindStringSubmatch(k8sNode.Name); matches != nil {
		return &node{
			Kind: "agent-" + matches[1],
			Node: k8sNode,
		}, nil
	}

	if controllerRegexp.MatchString(k8sNode.Name) {
		return &node{
			Kind: "control-plane",
			Node: k8sNode,
		}, nil
	}

	return nil, fmt.Errorf("%q is not a Raya node name", k8sNode.Name)
}

func main() {
	cfg, err := config.GetConfig()
	if err != nil {
		panic(err)
	}

	mgr, err := manager.New(cfg, manager.Options{})
	if err != nil {
		slog.Error("could not create a manager", "err", err)
		os.Exit(1)
	}

	k8s := mgr.GetClient()

	ctrl, err := controller.New("sisu-controller", mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctxt context.Context, _ reconcile.Request) (reconcile.Result, error) {
			return reconcile.Result{}, deleteZombies(ctxt, k8s)
		}),
	})
	if err != nil {
		slog.Error("could not create a controller", "err", err)
		os.Exit(1)
	}

	err = ctrl.Watch(source.Kind(mgr.GetCache(), &corev1.Node{},
		&handler.TypedEnqueueRequestForObject[*corev1.Node]{}))
	if err != nil {
		slog.Error("could not watch nodes", "err", err)
		os.Exit(1)
	}

	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		slog.Error("manager stopped", "err", err)
		os.Exit(1)
	}
}
