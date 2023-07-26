package main

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/slices"
)

// change below global variables to control the testing scale
var (
	col1Value = []int{1, 2, 3}
	col2Value = []string{"a", "b", "c"}
	col3Value = []int{11, 22, 33}

	numInsert = 5
)

type MVCCValue struct {
	ts       int
	value    string
	isDelete bool
}

type KvStore struct {
	m        map[string][]MVCCValue
	globalTS int
}

func NewKVStore() *KvStore {
	return &KvStore{
		m:        make(map[string][]MVCCValue),
		globalTS: 0,
	}
}

func (s *KvStore) set(key, value string) {
	// assuming each globalTS only have one row change
	s.globalTS++
	s.m[key] = append(s.m[key], MVCCValue{
		ts:       s.globalTS,
		value:    value,
		isDelete: false,
	})
}

type KVPair struct {
	key   string
	value string
}

func initializeKVStore(rows []string) *KvStore {
	store := NewKVStore()

	for _, r := range rows {
		tuple := strings.Split(r, ",")
		val1, val2, val3 := tuple[0], tuple[1], tuple[2]

		// for data KV, key will start with "r", followed by the real key
		newKey := fmt.Sprintf("r%s", val1)
		newValue := fmt.Sprintf("%s,%s,%s", val1, val2, val3)
		store.set(newKey, newValue)

		// for index KV, key will start with "i", followed by indexID, then the real index value (PK)
		newKey = fmt.Sprintf("i1_%s", val2)
		newValue = fmt.Sprintf("%s", val1)
		store.set(newKey, newValue)

		newKey = fmt.Sprintf("i2_%s", val3)
		newValue = fmt.Sprintf("%s", val1)
		store.set(newKey, newValue)
	}

	return store
}

func (s *KvStore) print() {
	for key, values := range s.m {
		fmt.Printf("key: %s\n", key)
		fmt.Println("value:")
		for _, value := range values {
			fmt.Println(value)
		}
	}
}

// convert data KV value to KV pairs that contain all the data KV and index KV of this data
func encodeKV(value string) []KVPair {
	KVPairs := make([]KVPair, 0, 3)
	tuple := strings.Split(value, ",")
	// encode data KV
	key := tuple[0]
	newKVPair := KVPair{
		"r" + key,
		value,
	}
	KVPairs = append(KVPairs, newKVPair)
	// encode index KV 1
	indexKey := "i1_" + tuple[1]
	newKVPair = KVPair{
		indexKey,
		key,
	}
	KVPairs = append(KVPairs, newKVPair)
	// encode index KV 2
	indexKey = "i2_" + tuple[2]
	newKVPair = KVPair{
		indexKey,
		key,
	}
	KVPairs = append(KVPairs, newKVPair)
	return KVPairs
}

func replaceConflict(store *KvStore) {
	// check index KV first
	for key, values := range store.m {
		if key[0] != 'i' {
			continue
		}
		if len(values) == 1 {
			continue
		}
		for i := 0; i < len(values)-1; i++ {
			overwritten := values[i]
			pk := "r" + overwritten.value
			rowKV, ok := store.getLatest(pk)
			if !ok {
				panic("should not happen")
			}
			if rowKV.isDelete {
				continue
			}
			encoded := encodeKV(rowKV.value)
			this := KVPair{
				key,
				overwritten.value,
			}
			if slices.Contains(encoded, this) {
				store.delete(pk)
			}
		}
	}
	// check data KV
	for key, values := range store.m {
		if key[0] != 'r' {
			continue
		}
		if len(values) == 1 {
			continue
		}
		latestValue := values[len(values)-1]
		toBeChecked := make([]MVCCValue, 0, len(values)-1)
		var mustKeep []KVPair
		if latestValue.isDelete {
			toBeChecked = append(toBeChecked, latestValue)
		} else {
			mustKeep = encodeKV(latestValue.value)
		}
		toBeChecked = append(toBeChecked, values[:len(values)-1]...)

		for _, v := range toBeChecked {
			if v.value == latestValue.value {
				continue
			}
			encodedKVs := encodeKV(v.value)
			for _, encodedKV := range encodedKVs {
				latestKV, ok := store.getLatest(encodedKV.key)
				if !ok {
					panic("should not happen")
				}
				if latestKV.isDelete {
					continue
				}
				if latestKV.value != encodedKV.value {
					continue
				}
				if slices.Contains(mustKeep, encodedKV) {
					continue
				}
				store.delete(encodedKV.key)
			}
		}
	}

}

func (s *KvStore) getLatest(key string) (MVCCValue, bool) {
	values, ok := s.m[key]
	if !ok {
		return MVCCValue{}, false
	}
	return values[len(values)-1], true
}

func (s *KvStore) delete(key string) {
	s.globalTS++
	s.m[key] = append(s.m[key], MVCCValue{
		ts:       s.globalTS,
		isDelete: true,
	})
}

func checkConsistConflict(store *KvStore) error {
	for key, values := range store.m {
		// find the value with maximum ts (the one with maximum ts is the real value of this key, others have been replaced by it)
		// since the MVCCValue struct with larger ts was appended to values later, we just need to retrieve the last one in values
		// if isDelete, continue to next key
		if values[len(values)-1].isDelete {
			continue
		}
		// check data KV
		if key[0] == 'r' {
			// find the value with maximum ts from values
			latestValue := values[len(values)-1].value
			kvs := encodeKV(latestValue)
			for _, kv := range kvs {
				latest, ok := store.getLatest(kv.key)
				// assert ok == true
				if !ok {
					return errors.Errorf(
						"KV pair {key: %s, value: %s} exists, but key %s does not exist",
						key, latestValue, kv.key,
					)
				}
				if latest.isDelete {
					return errors.Errorf(
						"KV pair {key: %s, value: %s} exists, but key %s is deleted",
						key, latestValue, kv.key,
					)
				}
				// assert latest value of latest should be same as kv.value
				if latest.value != kv.value {
					return errors.Errorf(
						"KV pair {key: %s, value: %s} exists, but the key %s is inconsistent. want %s, got %s",
						key, latestValue, kv.key, kv.value, latest.value,
					)
				}
			}
		}
		// check index KV
		if key[0] == 'i' {
			// find the value with maximum ts from values
			latestValue := values[len(values)-1].value
			// assert latestValue (PK) correctly exists in KVStore
			pkValue, ok := store.getLatest("r" + latestValue)
			if !ok {
				return errors.Errorf(
					"KV pair {key: %s, value: %s} exists, but key %s does not exist",
					key, latestValue, "r"+latestValue,
				)
			}
			if pkValue.isDelete {
				return errors.Errorf(
					"KV pair {key: %s, value: %s} exists, but key %s is deleted",
					key, latestValue, "r"+latestValue,
				)
			}
			// check the indexed value is in the PK's value. "ix_" has length 3
			// that is checking the orphan index KV
			if !slices.Contains(strings.Split(pkValue.value, ","), key[3:]) {
				return errors.Errorf(
					"KV pair {key: %s, value: %s} exists, but the indexed value %s is not in the PK's value %s",
					key, latestValue, key[3:], pkValue.value,
				)
			}
		}
	}
	return nil
}

func main() {
	now := time.Now()
	allRowsNum := len(col1Value) * len(col2Value) * len(col3Value)
	for i := uint64(0); i < uint64(math.Pow(float64(allRowsNum), float64(numInsert))); i++ {
		rows := make([]string, numInsert)
		cur := i
		for j := 0; j < numInsert; j++ {
			rowIndexes := cur % uint64(allRowsNum)
			cur = cur / uint64(allRowsNum)
			col1 := rowIndexes / uint64(len(col2Value)*len(col3Value))
			col2 := (rowIndexes % uint64(len(col2Value)*len(col3Value))) / uint64(len(col3Value))
			col3 := rowIndexes % uint64(len(col3Value))
			rows[j] = fmt.Sprintf("%v,%v,%v", col1Value[col1], col2Value[col2], col3Value[col3])
		}
		store := initializeKVStore(rows)
		if checkConsistConflict(store) == nil {
			continue
		}
		replaceConflict(store)
		err := checkConsistConflict(store)
		if err != nil {
			fmt.Println(rows)
			store.print()
			panic(fmt.Sprintf("checkConsistConflict failed: %+v", err))
		}
	}
	fmt.Println(time.Since(now))
}
