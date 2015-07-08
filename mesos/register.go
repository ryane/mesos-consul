package mesos

import (
	"encoding/json"
	"fmt"
	consulapi "github.com/hashicorp/consul/api"
	"log"
)

type cacheEntry struct {
	Service      *consulapi.AgentServiceRegistration
	IsRegistered bool
}

var cache map[string]*cacheEntry

func (m *Mesos) RegisterHosts(sj StateJSON) {
	log.Print("[INFO] Running RegisterHosts")

	// Register followers
	for _, f := range sj.Followers {
		h, p := parsePID(f.Pid)
		host := toIP(h)
		port := toPort(p)

		m.registerHost(&consulapi.AgentServiceRegistration{
			ID:      fmt.Sprintf("%s:%s", f.Id, f.Hostname),
			Name:    "mesos",
			Port:    port,
			Address: host,
			Tags:    []string{"follower"},
			Check: &consulapi.AgentServiceCheck{
				HTTP:     fmt.Sprintf("http://%s:%d/slave(1)/health", host, port),
				Interval: "10s",
			},
		})
	}

	// Register masters
	mas := m.getMasters()
	for _, ma := range mas {
		var tags []string

		if ma.isLeader {
			tags = []string{"leader", "master"}
		} else {
			tags = []string{"master"}
		}
		host := toIP(ma.host)
		port := toPort(ma.port)
		s := &consulapi.AgentServiceRegistration{
			ID:      fmt.Sprintf("mesos:%s:%s", ma.host, ma.port),
			Name:    "mesos",
			Port:    port,
			Address: host,
			Tags:    tags,
			Check: &consulapi.AgentServiceCheck{
				HTTP:     fmt.Sprintf("http://%s:%d/master/health", host, port),
				Interval: "10s",
			},
		}

		m.registerHost(s)
	}
}

// helper function to compare service tag slices
//
func sliceEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func (m *Mesos) registerHost(s *consulapi.AgentServiceRegistration) {

	if _, ok := cache[s.ID]; ok {
		log.Printf("[INFO] Host found. Comparing tags: (%v, %v)", cache[s.ID].Service.Tags, s.Tags)

		if sliceEq(s.Tags, cache[s.ID].Service.Tags) {
			cache[s.ID].IsRegistered = true

			// Tags are the same. Return
			return
		}

		log.Println("[INFO] Tags changed. Re-registering")

		// Delete cache entry. It will be re-created below
		delete(cache, s.ID)
	}

	cache[s.ID] = &cacheEntry{
		Service:      s,
		IsRegistered: true,
	}

	err := m.Consul.Register(s)
	if err != nil {
		log.Print("[ERROR] ", err)
	}
}

func (m *Mesos) register(s *consulapi.AgentServiceRegistration) {
	if _, ok := cache[s.ID]; ok {
		log.Printf("[INFO] Service found. Not registering: %s", s.ID)
		cache[s.ID].IsRegistered = true
		return
	}

	log.Print("[INFO] Registering ", s.ID)

	cache[s.ID] = &cacheEntry{
		Service:      s,
		IsRegistered: true,
	}

	err := m.Consul.Register(s)
	if err != nil {
		log.Print("[ERROR] ", err)
	}
}

// deregister items that have gone away
//
func (m *Mesos) deregister() {
	for s, b := range cache {
		if !b.IsRegistered {
			log.Print("[INFO] Deregistering ", s)
			err := m.Consul.Deregister(b.Service)
			if err != nil {
				log.Printf("[ERROR] could not deregister service %v: %v", b.Service.ID, err)
			}

			delete(cache, s)
		} else {
			cache[s].IsRegistered = false
		}
	}
}

func (m *Mesos) getCache() {
	if cache == nil {
		log.Printf("[DEBUG] getting cache from KV store.")

		kvp, err := m.Consul.Get("mesos-consul/cache")
		if err != nil {
			log.Print("[ERROR] Could not get cache from KV store: ", err)
		} else if kvp != nil {
			log.Printf("[DEBUG] found cache in KV store.")
			if err = json.Unmarshal([]byte(kvp.Value), &cache); err != nil {
				log.Print("[ERROR] Could not deserialize cache: ", err)
			} else {
				log.Printf("[DEBUG] populated cache from KV store.")
			}
		}
	}

	if cache == nil {
		log.Printf("[DEBUG] using empty cache.")
		cache = make(map[string]*cacheEntry)
	}
}

func (m *Mesos) saveCache() {
	log.Printf("[DEBUG] saving cache to KV store.")
	serializedCache, marshalErr := json.Marshal(&cache)
	if marshalErr != nil {
		log.Print("[ERROR] Could not serialize cache: ", marshalErr)
	}
	cacheKV := &consulapi.KVPair{
		Key:   "mesos-consul/cache",
		Value: serializedCache,
	}

	err := m.Consul.Put(cacheKV)
	if err != nil {
		log.Print("[ERROR] Could not save cache: ", err)
	}
}
