package main

import (
	"fmt"
	"strings"
)

type MVCCValue struct {
	ts       int
	value    string
	isDelete bool
}

type KVPair struct {
	key   string
	value string
}

var KVStore map[string][]MVCCValue

func initializeKVStore() {
	KVStore = make(map[string][]MVCCValue)
	globalTS := 1
	var col1Value = []int{1, 2, 3, 4}
	var col2Value = []string{"a", "b", "c", "d"}
	var col3Value = []int{11, 22, 33}
	for _, val1 := range col1Value {
		for _, val2 := range col2Value {
			for _, val3 := range col3Value {
				// for data KV, key will start with "r", followed by the real key
				newKey := fmt.Sprintf("r%d", val1)
				newValue := fmt.Sprintf("%d,%s,%d", val1, val2, val3)
				newMVCCValue := MVCCValue{
					globalTS,
					newValue,
					false,
				}
				KVStore[newKey] = append(KVStore[newKey], newMVCCValue)

				// for index KV, key will start with "i", followed by indexID, then the real index value (PK)
				newKey = fmt.Sprintf("i1_%s", val2)
				newValue = fmt.Sprintf("%d", val1)
				newMVCCValue = MVCCValue{
					globalTS,
					newValue,
					false,
				}
				KVStore[newKey] = append(KVStore[newKey], newMVCCValue)

				newKey = fmt.Sprintf("i2_%d", val3)
				newValue = fmt.Sprintf("%d", val1)
				newMVCCValue = MVCCValue{
					globalTS,
					newValue,
					false,
				}
				KVStore[newKey] = append(KVStore[newKey], newMVCCValue)

				globalTS++
			}
		}
	}
}

func printKVStore() {
	for key, values := range KVStore {
		fmt.Printf("key: %s", key)
		fmt.Println("value:")
		for _, value := range values {
			fmt.Println(value)
		}
	}
}

// convert data KV value to KV pairs that contain all the data KV and index KV of this data
func encodeKV(value string) []KVPair {
	KVPairs := make([]KVPair, 3)
	// encode data KV
	val1Index := strings.Index(value, ",")
	key := value[0:val1Index]
	newKVPair := KVPair{
		"r" + key,
		value,
	}
	KVPairs = append(KVPairs, newKVPair)
	// encode index KV 1
	newValue := value[val1Index+1:]
	val2Index := strings.Index(newValue, ",")
	indexKey := "i1_" + newValue[0:val2Index]
	newKVPair = KVPair{
		indexKey,
		key,
	}
	KVPairs = append(KVPairs, newKVPair)
	// encode index KV 2
	indexKey = "i2_" + newValue[val2Index+1:]
	newKVPair = KVPair{
		indexKey,
		key,
	}
	KVPairs = append(KVPairs, newKVPair)
	return KVPairs
}

func replaceConflict() {

}

func checkConsistConflict() bool {
	for key, values := range KVStore {
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
				valueInStore, ok := KVStore[kv.key]
				// assert ok == true
				if !ok {
					return true
				}
				// assert latest value of valueInStore should be same as kv.value
				if valueInStore[len(valueInStore)-1].isDelete || (valueInStore[len(valueInStore)-1].value != kv.value) {
					return true
				}
			}
		}
		// check index KV
		if key[0] == 'i' {
			// find the value with maximum ts from values
			latestValue := values[len(values)-1].value
			// assert latestValue (PK) correctly exists in KVStore
			PKValue, ok := KVStore["r"+latestValue]
			if !ok || PKValue[len(PKValue)-1].isDelete {
				return true
			}
		}
	}
	return false
}

func main() {
	initializeKVStore()
	replaceConflict()
	consistConflict := checkConsistConflict()
	if consistConflict {
		fmt.Println("KVStore consists data/index conflict.")
	} else {
		fmt.Println("KVStore does not consist data/index conflict.")
	}
}
