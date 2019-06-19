package structs

import (
	"sort"
	"strings"
)

// CompiledDiscoveryChain is the result from taking a set of related config
// entries for a single service's discovery chain and restructuring them into a
// form that is more usable for actual service discovery.
type CompiledDiscoveryChain struct {
	// Node is the top node in the chain.
	//
	// If this is a router or splitter then in envoy this renders as an http
	// route object.
	//
	// If this is a group resolver then in envoy this renders as a default
	// wildcard http route object.
	Node *DiscoveryNode `json:",omitempty"`

	// GroupResolverNodes respresents all unique service instance groups that
	// need to be represented. For envoy these render as Clusters.
	GroupResolverNodes map[DiscoveryTarget]*DiscoveryNode `json:",omitempty"`

	// TODO(rb): not sure if these two fields are actually necessary but I'll know when I get into xDS
	Resolvers map[string]*ServiceResolverConfigEntry `json:",omitempty"`
	Targets   []DiscoveryTarget                      `json:",omitempty"`
}

const (
	DiscoveryNodeTypeRouter        = "router"
	DiscoveryNodeTypeSplitter      = "splitter"
	DiscoveryNodeTypeGroupResolver = "group-resolver"
	DiscoveryNodeTypeCatalogQuery  = "catalog-query"
)

// DiscoveryNode is a single node of the compiled discovery chain.
type DiscoveryNode struct {
	Type string
	Name string // default chain/service name at this spot

	// fields for Type==router
	Routes []*DiscoveryRoute `json:",omitempty"`

	// fields for Type==splitter
	Splits []*DiscoverySplit `json:",omitempty"`

	// fields for Type==group-resolver
	GroupResolver *DiscoveryGroupResolver `json:",omitempty"`

	// fields for Type==catalog-query
	CatalogTarget DiscoveryTarget `json:",omitempty"`
}

// compiled form of ServiceResolverConfigEntry but customized per non-failover target
type DiscoveryGroupResolver struct {
	Definition *ServiceResolverConfigEntry `json:",omitempty"`
	Default    bool                        `json:",omitempty"`
	// Node should be of Type==catalog-query and is used in the happy path.
	Node     *DiscoveryNode     `json:",omitempty"`
	Failover *DiscoveryFailover `json:",omitempty"` // sad path
}

// compiled form of ServiceRoute
type DiscoveryRoute struct {
	Definition      *ServiceRoute  `json:",omitempty"`
	DestinationNode *DiscoveryNode `json:",omitempty"`
}

// compiled form of ServiceSplit
type DiscoverySplit struct {
	Weight float32        `json:",omitempty"`
	Node   *DiscoveryNode `json:",omitempty"`
}

// compiled form of ServiceResolverFailover
type DiscoveryFailover struct {
	Definition *ServiceResolverFailover `json:",omitempty"`
	Nodes      []*DiscoveryNode         `json:",omitempty"` // these must be of type catalog-query
}

// DiscoveryTarget represents all of the inputs necessary to use a resolver
// config entry to execute a catalog query to generate a list of service
// instances during discovery.
//
// This is a value type so it can be used as a map key.
type DiscoveryTarget struct {
	Service       string `json:",omitempty"`
	ServiceSubset string `json:",omitempty"`
	Namespace     string `json:",omitempty"`
	Datacenter    string `json:",omitempty"`
}

func (t DiscoveryTarget) IsEmpty() bool {
	return t.Service == "" && t.ServiceSubset == "" && t.Namespace == "" && t.Datacenter == ""
}

// CopyAndModify will duplicate the target and selectively modify it given the
// requested inputs.
func (t DiscoveryTarget) CopyAndModify(
	service,
	serviceSubset,
	namespace,
	datacenter string,
) DiscoveryTarget {
	t2 := t // copy
	if service != "" && service != t2.Service {
		t2.Service = service
		// Reset the chosen subset if we reference a service other than our own.
		t2.ServiceSubset = ""
	}
	if serviceSubset != "" && serviceSubset != t2.ServiceSubset {
		t2.ServiceSubset = serviceSubset
	}
	if namespace != "" && namespace != t2.Namespace {
		t2.Namespace = namespace
	}
	if datacenter != "" && datacenter != t2.Datacenter {
		t2.Datacenter = datacenter
	}
	return t2
}

func (t DiscoveryTarget) String() string {
	var b strings.Builder

	if t.ServiceSubset != "" {
		b.WriteString(t.ServiceSubset)
	} else {
		b.WriteString("<default>")
	}
	b.WriteRune('.')

	b.WriteString(t.Service)
	b.WriteRune('.')

	if t.Namespace != "" {
		b.WriteString(t.Namespace)
	} else {
		b.WriteString("default")
	}
	b.WriteRune('.')

	b.WriteString(t.Datacenter)

	return b.String()
}

type DiscoveryTargets []DiscoveryTarget

func (targets DiscoveryTargets) Sort() {
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].Service < targets[j].Service {
			return true
		} else if targets[i].Service > targets[j].Service {
			return false
		}

		if targets[i].ServiceSubset < targets[j].ServiceSubset {
			return true
		} else if targets[i].ServiceSubset > targets[j].ServiceSubset {
			return false
		}

		if targets[i].Namespace < targets[j].Namespace {
			return true
		} else if targets[i].Namespace > targets[j].Namespace {
			return false
		}

		return targets[i].Datacenter < targets[j].Datacenter
	})
}
