package discoverychain

import (
	"fmt"

	"github.com/hashicorp/consul/agent/structs"
)

// TODO(rb): surface any specific errors that may matter during graph vetting at write-time

// Compile assembles a discovery chain from raw config entries and local context.
func Compile(
	serviceName string,
	currentNamespace string,
	currentDatacenter string,
	entries *structs.DiscoveryChainConfigEntries,
) (*structs.CompiledDiscoveryChain, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("serviceName is required")
	}
	if currentNamespace == "" {
		return nil, fmt.Errorf("currentNamespace is required")
	}
	if currentDatacenter == "" {
		return nil, fmt.Errorf("currentDatacenter is required")
	}
	if entries == nil {
		return nil, fmt.Errorf("entries is required")
	}

	c := &compiler{
		serviceName:       serviceName,
		currentNamespace:  currentNamespace,
		currentDatacenter: currentDatacenter,
		entries:           entries,

		splitterNodes:      make(map[string]*structs.DiscoveryNode),
		groupResolverNodes: make(map[structs.DiscoveryTarget]*structs.DiscoveryNode),
		catalogQueryNodes:  make(map[structs.DiscoveryTarget]*structs.DiscoveryNode),

		resolvers:       make(map[string]*structs.ServiceResolverConfigEntry),
		retainResolvers: make(map[string]struct{}),
		targets:         make(map[structs.DiscoveryTarget]struct{}),
	}

	// Clone this resolver map to avoid mutating the input map during compilation.
	if len(entries.Resolvers) > 0 {
		for k, v := range entries.Resolvers {
			c.resolvers[k] = v
		}
	}

	return c.compile()
}

// compiler is a single-use struct for handling intermediate state necessary
// for assembling a discovery chain from raw config entries.
type compiler struct {
	serviceName       string
	currentNamespace  string
	currentDatacenter string

	// config entries that are being compiled (will be mutated during compilation)
	//
	// This is an INPUT field.
	entries *structs.DiscoveryChainConfigEntries

	// cached nodes
	splitterNodes      map[string]*structs.DiscoveryNode
	groupResolverNodes map[structs.DiscoveryTarget]*structs.DiscoveryNode // this is also an OUTPUT field
	catalogQueryNodes  map[structs.DiscoveryTarget]*structs.DiscoveryNode

	// topNode is computed inside of assembleChain()
	//
	// This is an OUTPUT field.
	topNode *structs.DiscoveryNode

	// resolvers is initially seeded by copying the provided entries.Resolvers
	// map and default resolvers are added as they are needed.
	//
	// If redirects cause a resolver to not be needed it will be omitted from
	// this map.
	//
	// This is an OUTPUT field.
	resolvers map[string]*structs.ServiceResolverConfigEntry
	// retainResolvers flags the elements of the resolvers map that should be
	// retained in the final results.
	retainResolvers map[string]struct{}

	// This is an OUTPUT field.
	targets map[structs.DiscoveryTarget]struct{}
}

func (c *compiler) compile() (*structs.CompiledDiscoveryChain, error) {
	if err := c.assembleChain(); err != nil {
		return nil, err
	}

	if c.topNode == nil {
		return nil, nil
	}

	if err := c.detectCircularSplits(); err != nil {
		return nil, err
	}
	if err := c.detectCircularResolves(); err != nil {
		return nil, err
	}

	c.mergeSerialSplitters()

	// Remove any unused resolvers.
	for name, _ := range c.resolvers {
		if _, ok := c.retainResolvers[name]; !ok {
			delete(c.resolvers, name)
		}
	}

	targets := make([]structs.DiscoveryTarget, 0, len(c.catalogQueryNodes))
	for target, _ := range c.catalogQueryNodes {
		targets = append(targets, target)
	}
	structs.DiscoveryTargets(targets).Sort()

	return &structs.CompiledDiscoveryChain{
		Node:               c.topNode,
		Resolvers:          c.resolvers,
		Targets:            targets,
		GroupResolverNodes: c.groupResolverNodes, // TODO: prune unused
	}, nil
}

func (c *compiler) detectCircularSplits() error {
	// TODO: detect when a tree of splitters backtracks
	return nil
}

func (c *compiler) detectCircularResolves() error {
	// TODO: detect when a series of redirects and failovers cause a circular reference
	return nil
}

func (c *compiler) mergeSerialSplitters() {
	for {
		anyChanged := false
		for _, splitterNode := range c.splitterNodes {
			fixedSplits := make([]*structs.DiscoverySplit, 0, len(splitterNode.Splits))
			changed := false
			for _, split := range splitterNode.Splits {
				if split.Node.Type != structs.DiscoveryNodeTypeSplitter {
					fixedSplits = append(fixedSplits, split)
					continue
				}

				changed = true

				for _, innerSplit := range split.Node.Splits {
					effectiveWeight := split.Weight * innerSplit.Weight / 100

					newDiscoverySplit := &structs.DiscoverySplit{
						Weight: structs.NormalizeServiceSplitWeight(effectiveWeight),
						Node:   innerSplit.Node,
					}

					fixedSplits = append(fixedSplits, newDiscoverySplit)
				}
			}

			if changed {
				splitterNode.Splits = fixedSplits
				anyChanged = true
			}
		}

		if !anyChanged {
			return
		}
	}
}

// assembleChain will do the initial assembly of a chain of DiscoveryNode
// entries from the provided config entries.  No default resolvers are injected
// here so it is expected that if there are no discovery chain config entries
// set up for a given service that it will produce no topNode from this.
func (c *compiler) assembleChain() error {
	if c.topNode != nil {
		return fmt.Errorf("assembleChain should only be called once")
	}

	if c.entries.IsEmpty() {
		return nil // nothing explicitly configured
	}

	// The only router we consult is the one for the service name at the top of
	// the chain.
	router, ok := c.entries.Routers[c.serviceName]
	if !ok {
		// If no router is configured, move on down the line to the next hop of
		// the chain.
		c.topNode = c.getSplitterOrGroupResolverNode(c.newTarget(c.serviceName, "", "", ""))
		return nil
	}

	routeNode := &structs.DiscoveryNode{
		Type:   structs.DiscoveryNodeTypeRouter,
		Name:   router.Name,
		Routes: make([]*structs.DiscoveryRoute, 0, len(router.Routes)+1),
	}

	for i, _ := range router.Routes {
		// We don't use range variables here because we'll take the address of
		// this route and store that in a DiscoveryNode and the range variables
		// share memory addresses between iterations which is exactly wrong for
		// us here.
		route := router.Routes[i]

		compiledRoute := &structs.DiscoveryRoute{Definition: &route}
		routeNode.Routes = append(routeNode.Routes, compiledRoute)

		dest := route.Destination

		svc := defaultIfEmpty(dest.Service, c.serviceName)

		// Check to see if the destination is eligible for splitting.
		if dest.ServiceSubset == "" && dest.Namespace == "" {
			compiledRoute.DestinationNode = c.getSplitterOrGroupResolverNode(
				c.newTarget(svc, dest.ServiceSubset, dest.Namespace, ""),
			)
		} else {
			compiledRoute.DestinationNode = c.getGroupResolverNode(
				c.newTarget(svc, dest.ServiceSubset, dest.Namespace, ""),
				false,
			)
		}
	}

	// If we have a router, we'll add a catch-all route at the end to send
	// unmatched traffic to the next hop in the chain.
	defaultRoute := &structs.DiscoveryRoute{
		Definition:      newDefaultServiceRoute(c.serviceName),
		DestinationNode: c.getSplitterOrGroupResolverNode(c.newTarget(c.serviceName, "", "", "")),
	}
	routeNode.Routes = append(routeNode.Routes, defaultRoute)

	c.topNode = routeNode
	return nil
}

func newDefaultServiceRoute(serviceName string) *structs.ServiceRoute {
	return &structs.ServiceRoute{
		Match: &structs.ServiceRouteMatch{
			HTTP: &structs.ServiceRouteHTTPMatch{
				PathPrefix: "/",
			},
		},
		Destination: &structs.ServiceRouteDestination{
			Service: serviceName,
		},
	}
}

func (c *compiler) newTarget(service, serviceSubset, namespace, datacenter string) structs.DiscoveryTarget {
	if service == "" {
		panic("newTarget called with empty service which makes no sense")
	}
	return structs.DiscoveryTarget{
		Service:       service,
		ServiceSubset: serviceSubset,
		Namespace:     defaultIfEmpty(namespace, c.currentNamespace),
		Datacenter:    defaultIfEmpty(datacenter, c.currentDatacenter),
	}
}

func (c *compiler) getSplitterOrGroupResolverNode(target structs.DiscoveryTarget) *structs.DiscoveryNode {
	nextNode := c.getSplitterNode(target.Service)
	if nextNode != nil {
		return nextNode
	}
	return c.getGroupResolverNode(target, false)
}

func (c *compiler) getSplitterNode(name string) *structs.DiscoveryNode {
	// Do we already have the node?
	if prev, ok := c.splitterNodes[name]; ok {
		return prev
	}

	// Fetch the config entry.
	splitter, ok := c.entries.Splitters[name]
	if !ok {
		return nil
	}

	// Build node.
	splitNode := &structs.DiscoveryNode{
		Type:   structs.DiscoveryNodeTypeSplitter,
		Name:   name,
		Splits: make([]*structs.DiscoverySplit, 0, len(splitter.Splits)),
	}

	// If we record this exists before recursing down it will short-circuit
	// sanely if there is some sort of graph loop below.
	c.splitterNodes[name] = splitNode

	for _, split := range splitter.Splits {
		compiledSplit := &structs.DiscoverySplit{
			Weight: split.Weight,
		}
		splitNode.Splits = append(splitNode.Splits, compiledSplit)

		svc := defaultIfEmpty(split.Service, name)
		// Check to see if the split is eligible for additional splitting.
		if svc != name && split.ServiceSubset == "" && split.Namespace == "" {
			if nextNode := c.getSplitterNode(svc); nextNode != nil {
				// TODO: merge serial splitter nodes into one
				compiledSplit.Node = nextNode
				continue
			}
			// fall through to group-resolver
		}

		compiledSplit.Node = c.getGroupResolverNode(
			c.newTarget(svc, split.ServiceSubset, split.Namespace, ""),
			false,
		)
	}

	return splitNode
}

// getGroupResolverNode handles most of the code to handle
// redirection/rewriting capabilities from a resolver config entry. It recurses
// into itself to _generate_ catalog query nodes used for failover out of
// convenience.
func (c *compiler) getGroupResolverNode(target structs.DiscoveryTarget, recursedForFailover bool) *structs.DiscoveryNode {
RESOLVE_AGAIN:
	// Do we already have the node?
	if prev, ok := c.groupResolverNodes[target]; ok {
		return prev
	}

	// Fetch the config entry.
	resolver, ok := c.resolvers[target.Service]
	if !ok {
		// Materialize defaults and cache.
		resolver = newDefaultServiceResolver(target.Service)
		c.resolvers[target.Service] = resolver
	}

	// Handle redirects right up front.
	if resolver.Redirect != nil {
		redirect := resolver.Redirect

		redirectedTarget := target.CopyAndModify(
			redirect.Service,
			redirect.ServiceSubset,
			redirect.Namespace,
			redirect.Datacenter,
		)
		if redirectedTarget != target {
			target = redirectedTarget
			goto RESOLVE_AGAIN
		}
	}

	// Handle default subset.
	if target.ServiceSubset == "" && resolver.DefaultSubset != "" {
		target.ServiceSubset = resolver.DefaultSubset
		goto RESOLVE_AGAIN
	}

	// Since we're actually building a node with it, we can keep it.
	//
	// TODO: maybe infer this from the keyspace of the groupresolvernodes
	// slice.
	c.retainResolvers[target.Service] = struct{}{}

	if target.Service != resolver.Name {
		//TODO: remove
		panic("NOT POSSIBLE")
	}

	// Build node.
	groupResolverNode := &structs.DiscoveryNode{
		Type: structs.DiscoveryNodeTypeGroupResolver,
		Name: resolver.Name,
		GroupResolver: &structs.DiscoveryGroupResolver{
			Definition: resolver,
			Default:    resolver.IsDefault(),
			Node:       c.getCatalogQueryNode(target),
		},
	}
	groupResolver := groupResolverNode.GroupResolver

	if recursedForFailover {
		// If we recursed here from ourselves in a failover context, just emit
		// this node without caching it or even processing failover again.
		// This is a little weird but it keeps the redirect/default-subset
		// logic in one place.
		return groupResolverNode
	}

	// If we record this exists before recursing down it will short-circuit
	// sanely if there is some sort of graph loop below.
	c.groupResolverNodes[target] = groupResolverNode

	if len(resolver.Failover) > 0 {
		f := resolver.Failover

		// Determine which failover section applies.
		failover, ok := f[target.ServiceSubset]
		if !ok {
			failover, ok = f["*"]
		}

		if ok {
			// Determine which failover definitions apply.
			var failoverTargets []structs.DiscoveryTarget
			if len(failover.Datacenters) > 0 {
				for _, dc := range failover.Datacenters {
					// Rewrite the target as per the failover policy.
					failoverTarget := target.CopyAndModify(
						failover.Service,
						failover.ServiceSubset,
						failover.Namespace,
						dc,
					)
					if failoverTarget != target { // don't failover to yourself
						failoverTargets = append(failoverTargets, failoverTarget)
					}
				}
			} else {
				// Rewrite the target as per the failover policy.
				failoverTarget := target.CopyAndModify(
					failover.Service,
					failover.ServiceSubset,
					failover.Namespace,
					"",
				)
				if failoverTarget != target { // don't failover to yourself
					failoverTargets = append(failoverTargets, failoverTarget)
				}
			}

			// If we filtered everything out then no point in having a failover.
			if len(failoverTargets) > 0 {
				df := &structs.DiscoveryFailover{
					Definition: &failover,
				}
				groupResolver.Failover = df

				// Convert the targets into catalog query nodes by cheating a
				// bit and recursing into ourselves.
				for _, target := range failoverTargets {
					failoverGroupResolverNode := c.getGroupResolverNode(target, true)
					if failoverGroupResolverNode.Type != structs.DiscoveryNodeTypeGroupResolver {
						panic("TODO(remove): '" + failoverGroupResolverNode.Type + "' is not a group-resolver node")
					}
					catalogQueryNode := failoverGroupResolverNode.GroupResolver.Node
					if catalogQueryNode.Type != structs.DiscoveryNodeTypeCatalogQuery {
						panic("TODO(remove): not a catalog query node")
					}
					df.Nodes = append(df.Nodes, catalogQueryNode)
				}
			}
		}
	}

	return groupResolverNode
}

func (c *compiler) getCatalogQueryNode(target structs.DiscoveryTarget) *structs.DiscoveryNode {
	node, ok := c.catalogQueryNodes[target]
	if !ok {
		node = &structs.DiscoveryNode{
			Type:          structs.DiscoveryNodeTypeCatalogQuery,
			Name:          target.Service,
			CatalogTarget: target,
		}
		c.catalogQueryNodes[target] = node
	}
	return node
}

func newDefaultServiceResolver(serviceName string) *structs.ServiceResolverConfigEntry {
	return &structs.ServiceResolverConfigEntry{
		Kind: structs.ServiceResolver,
		Name: serviceName,
	}
}

func defaultIfEmpty(val, defaultVal string) string {
	if val != "" {
		return val
	}
	return defaultVal
}
