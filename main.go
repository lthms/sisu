package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sort"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	crlog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var agentRegexp = regexp.MustCompile(`^agent-(0|[1-9][0-9]*)-[0-9a-f]{8}$`)
var controllerRegexp = regexp.MustCompile(`^control-plane-[0-9a-f]{8}$`)

func configureLogging(output io.Writer, level slog.Level) {
	handler := slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
	crlog.SetLogger(logr.FromSlogHandler(handler))
}

func nodesByKind(ctx context.Context, k8s client.Client) (map[string][]node, error) {
	var nodes corev1.NodeList
	if err := k8s.List(ctx, &nodes); err != nil {
		return nil, fmt.Errorf("could not list nodes: %w", err)
	}

	byKind := make(map[string][]node)
	for i := range nodes.Items {
		if !nodes.Items[i].DeletionTimestamp.IsZero() {
			continue
		}
		parsed, err := New(&nodes.Items[i])
		if err != nil {
			slog.Warn("ignoring node with unrecognized name", "node", nodes.Items[i].Name)
			continue
		}
		byKind[parsed.Kind] = append(byKind[parsed.Kind], *parsed)
	}

	for _, group := range byKind {
		sort.SliceStable(group, func(i, j int) bool {
			return group[i].Node.CreationTimestamp.After(group[j].Node.CreationTimestamp.Time)
		})
	}

	return byKind, nil
}

func zombieNodes(byKind map[string][]node) []node {
	var others []node
	for kind, group := range byKind {
		if len(group) < 2 {
			slog.Debug("no duplicate nodes for kind", "kind", kind)
			continue
		}

		newest := group[0]
		ready := false
		for _, condition := range newest.Node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			slog.Info("waiting for newest node to be Ready", "kind", kind, "node", newest.Node.Name)
			continue
		}
		for _, candidate := range group[1:] {
			slog.Info("stale node selected for cleanup", "kind", kind, "node", candidate.Node.Name, "replacement", newest.Node.Name)
			others = append(others, candidate)
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
		if !attachment.DeletionTimestamp.IsZero() {
			continue
		}
		slog.Info("requesting volume attachment deletion", "node", n.Node.Name, "attachment", attachment.Name)
		if err := k8s.Delete(ctx, attachment); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("could not delete volume attachment %q for node %q: %w", attachment.Name, n.Node.Name, err)
			}
			slog.Info("volume attachment already absent", "node", n.Node.Name, "attachment", attachment.Name)
		} else {
			slog.Info("volume attachment deletion accepted", "node", n.Node.Name, "attachment", attachment.Name)
		}
	}

	return nil
}

// Cleanup errors are returned to controller-runtime for logging and retries.
func deleteZombies(ctx context.Context, k8s client.Client) error {
	byKind, err := nodesByKind(ctx, k8s)
	if err != nil {
		return err
	}

	zombies := zombieNodes(byKind)
	slog.Debug("node scan completed", "kinds", len(byKind), "stale_nodes", len(zombies))
	for _, zombie := range zombies {
		if err := zombie.deleteVolumeAttachments(ctx, k8s); err != nil {
			return err
		}
		slog.Info("requesting stale node deletion", "node", zombie.Node.Name, "kind", zombie.Kind)
		if err := k8s.Delete(ctx, zombie.Node); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("could not delete zombie node %q: %w", zombie.Node.Name, err)
			}
			slog.Info("stale node already absent", "node", zombie.Node.Name)
		} else {
			slog.Info("stale node deletion accepted", "node", zombie.Node.Name, "kind", zombie.Kind)
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
	var level slog.Level
	flag.TextVar(&level, "log-level", slog.LevelInfo, "Log level: debug, info, warn, or error")
	flag.Parse()
	configureLogging(os.Stderr, level)
	slog.Info("starting sisu", "log_level", level.String())

	cfg, err := config.GetConfig()
	if err != nil {
		slog.Error("could not load Kubernetes configuration", "err", err)
		os.Exit(1)
	}

	mgr, err := manager.New(cfg, manager.Options{
		Client: client.Options{
			Cache: &client.CacheOptions{
				// Keep watches cached, but read cleanup targets directly from
				// the API server to observe accepted deletions on the next scan.
				DisableFor: []client.Object{&corev1.Node{}, &storagev1.VolumeAttachment{}},
			},
		},
	})
	if err != nil {
		slog.Error("could not create a manager", "err", err)
		os.Exit(1)
	}

	k8s := mgr.GetClient()

	ctrl, err := controller.New("sisu-controller", mgr, controller.Options{
		Reconciler: reconcile.Func(func(ctxt context.Context, request reconcile.Request) (reconcile.Result, error) {
			slog.Debug("reconciling node event", "node", request.Name)
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
	slog.Info("sisu stopped")
}
