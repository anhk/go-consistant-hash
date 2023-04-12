package main

import (
	"fmt"
	"sort"
	"sync"
)

type uints []uint32 // 用来保存圆环上的点

// 实现 sort.Interface 接口
func (u uints) Len() int {
	return len(u)
}

func (u uints) Less(i, j int) bool {
	return u[i] < u[j]
}

func (u uints) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type ConsistantHash struct {
	Replicas int // 副本数量
	Nodes    map[string]struct{}
	Servers  map[uint32]string
	Circle   uints
	sync.RWMutex
}

func (hash *ConsistantHash) add(slot string) {
	for i := 0; i < hash.Replicas; i++ {
		key := FNVHash(fmt.Sprintf("%v#%v", slot, i))
		hash.Circle = append(hash.Circle, key)
		hash.Servers[key] = slot
	}

	hash.Nodes[slot] = struct{}{}
	sort.Sort(hash.Circle)
}

func (hash *ConsistantHash) Add(slot string) {
	hash.Lock()
	defer hash.Unlock()

	hash.add(slot)
}

func (hash *ConsistantHash) Get(name string) string {
	hash.RLock()
	defer hash.RUnlock()

	key := FNVHash(name)
	i := sort.Search(len(hash.Circle), func(i int) bool {
		return hash.Circle[i] >= key
	})
	fmt.Println("=======")
	fmt.Println("key:", key)
	fmt.Println("i:", i)

	if i >= hash.Circle.Len() {
		i = 0
	}
	return hash.Servers[hash.Circle[i]]
}

func main() {
	ch := &ConsistantHash{
		Replicas: 32,
		Nodes:    make(map[string]struct{}),
		Servers:  make(map[uint32]string),
		Circle:   make([]uint32, 0),
	}

	ch.Add("192.168.0.1")
	ch.Add("192.168.0.2")
	ch.Add("192.168.0.3")
	ch.Add("192.168.0.4")
	ch.Add("192.168.0.5")

	fmt.Println(ch.Circle)

	fmt.Println(ch.Get("a"))
	fmt.Println(ch.Get("hellowor32ld"))
	fmt.Println(ch.Get("11helloworld"))
	fmt.Println(ch.Get("a4asdf"))
	fmt.Println(ch.Get("rsgw"))
	fmt.Println(ch.Get("4"))
	fmt.Println(ch.Get("a"))
	fmt.Println(ch.Get("asdf"))
	fmt.Println(ch.Get("44"))
}

func FNVHash(name string) uint32 {
	p := uint32(16777619)
	hash := uint32(2166136261)

	for i := 0; i < len(name); i++ {
		hash = (hash ^ uint32(name[i])) * p
	}
	hash += hash << 13
	hash ^= hash >> 7
	hash += hash << 3
	hash ^= hash >> 17
	hash += hash << 5
	return hash
}
