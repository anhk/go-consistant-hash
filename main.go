package main

import (
	"fmt"
	"hash/fnv"
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
	circle   uints
	sync.RWMutex
}

func (hash *ConsistantHash) add(slot string) {
	for i := 0; i < hash.Replicas; i++ {
		key := FNVHash(slot)
		hash.circle = append(hash.circle, key)
		hash.Servers[key] = slot
	}

	hash.Nodes[slot] = struct{}{}
	sort.Sort(hash.circle)
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
	i := sort.Search(len(hash.circle), func(i int) bool {
		return hash.circle[i] >= key
	})
	return hash.Servers[hash.circle[i]]
}

func main() {
	ch := &ConsistantHash{
		Replicas: 32,
		Nodes:    make(map[string]struct{}),
		Servers:  make(map[uint32]string),
		circle:   make([]uint32, 0),
	}

	ch.Add("192.168.0.1")
	ch.Add("192.168.0.2")
	ch.Add("192.168.0.3")
	ch.Add("192.168.0.4")
	ch.Add("192.168.0.5")

	fmt.Println(ch.Get("helloworld"))
	fmt.Println(ch.Get("helloworld"))
	fmt.Println(ch.Get("helloworld"))
}

// 默认的hash函数
// 测试的发现 fnv hash 函数对于 key 相差不多的
// 映射出来的 uint32 值十分相近
func FNVHash(name string) uint32 {
	f := fnv.New32()
	f.Write([]byte(name))
	return f.Sum32()
}
