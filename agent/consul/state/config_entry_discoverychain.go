package state

import (
	"fmt"

	"github.com/hashicorp/consul/agent/structs"
	memdb "github.com/hashicorp/go-memdb"
)

// ReadDiscoveryChainConfigEntries will query for the full discovery chain for
// the provided service name. All relevant config entries will be recursively
// fetched and included in the result.
//
// Once returned, the caller still needs to assemble these into a useful graph
// structure.
func (s *Store) ReadDiscoveryChainConfigEntries(
	ws memdb.WatchSet,
	serviceName string,
) (uint64, *structs.DiscoveryChainConfigEntries, error) {
	tx := s.db.Txn(false)
	defer tx.Abort()
	return s.readDiscoveryChainConfigEntriesTxn(tx, ws, serviceName)
}

func (s *Store) readDiscoveryChainConfigEntriesTxn(
	tx *memdb.Txn,
	ws memdb.WatchSet,
	serviceName string,
) (uint64, *structs.DiscoveryChainConfigEntries, error) {
	// TODO: improve this so you can simulate changes to vet writes.

	// first fetch the router, of which we only collect 1 per chain eval
	idx, router, err := s.getRouterConfigEntryTxn(tx, ws, serviceName)
	if err != nil {
		return 0, nil, err
	}

	// Note that below we always look up splitters and resolvers in pairs, even
	// in some circumstances where both are not strictly necessary.
	//
	// For now we'll just eat the cost of fetching pairs of splitter/resolver
	// config entries even though we may not always need both. In the common
	// case we will need the pair so there's not a big drive to optimize this
	// here at this time.

	res := &structs.DiscoveryChainConfigEntries{
		Routers: map[string]*structs.ServiceRouterConfigEntry{
			serviceName: router,
		},
		Splitters: make(map[string]*structs.ServiceSplitterConfigEntry),
		Resolvers: make(map[string]*structs.ServiceResolverConfigEntry),
	}

	// Both Splitters and Resolvers maps will contain placeholder nils until
	// the end of this function to indicate "no such entry".

	var (
		todoSplitters = make(map[string]struct{})
		todoResolvers = make(map[string]struct{})
	)

	if router != nil {
		for _, svc := range router.ListRelatedServices() {
			todoSplitters[svc] = struct{}{}
		}
	} else {
		// Next hop in the chain is the splitter.
		todoSplitters[serviceName] = struct{}{}
	}

	for {
		name, ok := anyKey(todoSplitters)
		if !ok {
			break
		}
		delete(todoSplitters, name)

		if _, ok := res.Splitters[name]; ok {
			continue // already fetched
		}

		_, splitter, err := s.getSplitterConfigEntryTxn(tx, ws, name)
		if err != nil {
			return 0, nil, err
		}

		if splitter == nil {
			res.Splitters[name] = nil

			// Next hop in the chain is the resolver.
			todoResolvers[name] = struct{}{}
			continue
		}

		if len(splitter.Splits) == 0 {
			return 0, nil, fmt.Errorf("found splitter config for %q that has no splits", name)
		}

		res.Splitters[name] = splitter

		for _, svc := range splitter.ListRelatedServices() {
			// If there is no splitter, this will end up adding a resolver
			// after another iteration.
			todoSplitters[svc] = struct{}{}
		}
	}

	// Strip nils.
	for name, splitter := range res.Splitters {
		if splitter == nil {
			delete(res.Splitters, name)
		}
	}

	for {
		name, ok := anyKey(todoResolvers)
		if !ok {
			break
		}
		delete(todoResolvers, name)

		if _, ok := res.Resolvers[name]; ok {
			continue // already fetched
		}

		_, resolver, err := s.getResolverConfigEntryTxn(tx, ws, name)
		if err != nil {
			return 0, nil, err
		}

		if resolver == nil {
			res.Resolvers[name] = nil
			continue
		}

		if len(resolver.Failover) > 0 {
			for subset, failoverClause := range resolver.Failover {
				if failoverClause.Service == "" &&
					failoverClause.ServiceSubset == "" &&
					failoverClause.Namespace == "" &&
					len(failoverClause.Datacenters) == 0 {
					return 0, nil, fmt.Errorf("failover section for subset %q is errantly empty", subset)
				}
			}
		}

		res.Resolvers[name] = resolver

		for _, svc := range resolver.ListRelatedServices() {
			todoResolvers[svc] = struct{}{}
		}
	}

	// Strip nils.
	//
	// TODO: default resolver done here?
	// NOTE: for now when processing this enclosing struct we will inject defaults as needed
	for name, resolver := range res.Resolvers {
		if resolver == nil {
			delete(res.Resolvers, name)
		}
	}

	return idx, res, nil
}

// anyKey returns any key from the provided map if any exist. Useful for using
// a map as a simple work queue of sorts.
func anyKey(m map[string]struct{}) (string, bool) {
	if len(m) == 0 {
		return "", false
	}
	for k, _ := range m {
		return k, true
	}
	return "", false
}

// getRouterConfigEntryTxn is a convenience method for fetching a
// service-router kind of config entry.
func (s *Store) getRouterConfigEntryTxn(tx *memdb.Txn, ws memdb.WatchSet, serviceName string) (uint64, *structs.ServiceRouterConfigEntry, error) {
	idx, entry, err := s.configEntryTxn(tx, ws, structs.ServiceRouter, serviceName)
	if err != nil {
		return 0, nil, err
	} else if entry == nil {
		return idx, nil, nil
	}

	router, ok := entry.(*structs.ServiceRouterConfigEntry)
	if !ok {
		return 0, nil, fmt.Errorf("invalid service config type %T", entry)
	}
	return idx, router, nil
}

// getSplitterConfigEntryTxn is a convenience method for fetching a
// service-splitter kind of config entry.
func (s *Store) getSplitterConfigEntryTxn(tx *memdb.Txn, ws memdb.WatchSet, serviceName string) (uint64, *structs.ServiceSplitterConfigEntry, error) {
	idx, entry, err := s.configEntryTxn(tx, ws, structs.ServiceSplitter, serviceName)
	if err != nil {
		return 0, nil, err
	} else if entry == nil {
		return idx, nil, nil
	}

	splitter, ok := entry.(*structs.ServiceSplitterConfigEntry)
	if !ok {
		return 0, nil, fmt.Errorf("invalid service config type %T", entry)
	}
	return idx, splitter, nil
}

// getResolverConfigEntryTxn is a convenience method for fetching a
// service-resolver kind of config entry.
func (s *Store) getResolverConfigEntryTxn(tx *memdb.Txn, ws memdb.WatchSet, serviceName string) (uint64, *structs.ServiceResolverConfigEntry, error) {
	idx, entry, err := s.configEntryTxn(tx, ws, structs.ServiceResolver, serviceName)
	if err != nil {
		return 0, nil, err
	} else if entry == nil {
		return idx, nil, nil
	}

	resolver, ok := entry.(*structs.ServiceResolverConfigEntry)
	if !ok {
		return 0, nil, fmt.Errorf("invalid service config type %T", entry)
	}
	return idx, resolver, nil
}
